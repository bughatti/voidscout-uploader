package main

// WoW Forever fights (step 1 of keeping retail and Forever apart, 2026-11-06).
//
// Forever has its own install folder (_forever_) and its own VoidScout SavedVariables.
// Its fights go to /api/forever/ingest/fight, which stores them apart from retail.
//
// Only fights go up from Forever. Player profiles, combat logs and raid-tool sessions
// stay retail-only, and the retail file finder never looks in _forever_: it picks the
// newest VoidScout.lua it finds, so a Forever file would otherwise win after a Forever
// session and its fights would land in the retail scores.
//
// The Forever loop keeps its own small state file (forever_state.json): its "uploaded up
// to" bookmark and its own opt-out acknowledgement. Nothing is shared with the retail
// State, which other goroutines read and write.

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	foreverScanInterval  = 60 * time.Second
	foreverStateFileName = "forever_state.json"
)

type foreverState struct {
	LastUploadedTs int64 `json:"last_uploaded_ts"` // upload Forever fights newer than this
	LastOptOutTs   int64 `json:"last_opt_out_ts"`  // last Forever opt-out request sent
}

func foreverStatePath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, stateDirName, foreverStateFileName)
}

func loadForeverState() *foreverState {
	st := &foreverState{}
	if b, err := os.ReadFile(foreverStatePath()); err == nil {
		_ = json.Unmarshal(b, st)
	}
	return st
}

func saveForeverState(st *foreverState) error {
	p := foreverStatePath()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(st, "", "  ")
	return os.WriteFile(p, b, 0o644)
}

// isForeverPath reports whether a SavedVariables path belongs to a WoW Forever install:
// any folder in its absolute path is named _forever_.
func isForeverPath(p string) bool {
	if abs, err := filepath.Abs(p); err == nil {
		p = abs
	}
	for _, part := range strings.FieldsFunc(filepath.ToSlash(p), func(r rune) bool { return r == '/' }) {
		if strings.EqualFold(part, "_forever_") {
			return true
		}
	}
	return false
}

// foreverInstallPaths: the _forever_ folder next to each place we look for _retail_.
func foreverInstallPaths() []string {
	var out []string
	for _, retail := range allRetailCandidates() {
		p := filepath.Join(filepath.Dir(retail), "_forever_")
		if st, err := os.Stat(p); err == nil && st.IsDir() {
			out = append(out, p)
		}
	}
	return out
}

// detectForeverSavedVariablesPath finds the newest VoidScout.lua under a _forever_ install.
func detectForeverSavedVariablesPath() string {
	var best string
	var bestMtime time.Time
	for _, install := range foreverInstallPaths() {
		entries, err := os.ReadDir(filepath.Join(install, "WTF", "Account"))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			p := filepath.Join(install, "WTF", "Account", e.Name(), "SavedVariables", "VoidScout.lua")
			if st, err := os.Stat(p); err == nil && (best == "" || st.ModTime().After(bestMtime)) {
				best, bestMtime = p, st.ModTime()
			}
		}
	}
	return best
}

// runForeverOnce uploads the Forever fights newer than the Forever bookmark. It follows
// the consent saved in the FOREVER addon (each game's VoidScout asks for its own) and never
// touches the retail consent flags the profile and combat-log loops use. Returns false when
// something should be tried again (a failed upload or opt-out), so the loop retries it.
func runForeverOnce(svPath, apiBase string, dryRun, verbose bool) bool {
	fst := loadForeverState()
	ok := true
	if !dryRun {
		// A "Delete + go local" asked for in Forever, acknowledged in Forever's own state.
		ack := &State{LastOptOutTs: fst.LastOptOutTs}
		checkAndSendOptOut(svPath, apiBase, ack)
		if ack.LastOptOutTs != fst.LastOptOutTs {
			fst.LastOptOutTs = ack.LastOptOutTs
			if err := saveForeverState(fst); err != nil {
				log.Printf("forever: warning: failed to persist state: %v", err)
			}
		} else if req, _ := parseOptOutRequest(svPath); req != nil && req.RequestedAt > fst.LastOptOutTs {
			ok = false // the opt-out didn't go through yet: try again next pass
		}
	}
	if allowed, _ := readConsent(svPath); !allowed {
		if verbose {
			log.Printf("forever: uploads are off in Forever's VoidScout (local-only or no choice yet): nothing sent")
		}
		return ok
	}
	fights, contributor, err := parseSavedVars(svPath)
	if err != nil {
		log.Printf("forever: parse error: %v", err)
		return false
	}
	pending := filterPending(fights, fst.LastUploadedTs)
	if len(pending) == 0 {
		if verbose {
			log.Printf("forever: nothing new to upload (lastTs=%d)", fst.LastUploadedTs)
		}
		return ok
	}
	log.Printf("forever: found %d new fights, uploading as %s", len(pending), contributor)
	if dryRun {
		for _, f := range pending {
			log.Printf("  [DRY forever] %s-%s %s diff=%d outcome=%s axes=%v", f.PlayerName, f.PlayerRealm, f.EncounterName, f.DifficultyID, f.Outcome, f.Axes)
		}
		return ok
	}
	maxTs := fst.LastUploadedTs
	allSent := true
	for i := 0; i < len(pending); i += batchSize {
		end := min(i+batchSize, len(pending))
		batch := pending[i:end]
		resp, err := uploadBatchTo(apiBase, "/api/forever/ingest/fight", contributor, batch)
		if err != nil {
			log.Printf("forever: upload failed (batch %d-%d): %v", i, end, err)
			ok, allSent = false, false
			break
		}
		log.Printf("forever: uploaded %d accepted=%d skipped=%d reasons=%v",
			len(batch), resp.FightsAccepted, resp.FightsSkipped, resp.SkipReasons)
		if resp.SkipReasons["db_error"] > 0 {
			// The server couldn't store some of them: send this pass again.
			ok, allSent = false, false
			break
		}
		for _, f := range batch {
			if f.Timestamp > maxTs {
				maxTs = f.Timestamp
			}
		}
	}
	// The bookmark only moves when every batch went through: one fight's player rows can
	// straddle two batches with the same timestamp, so a partial advance could skip some.
	// Re-sent fights the server already has come back as "duplicate".
	if allSent && maxTs > fst.LastUploadedTs {
		fst.LastUploadedTs = maxTs
		if err := saveForeverState(fst); err != nil {
			log.Printf("forever: warning: failed to persist state: %v", err)
		}
	}
	return ok
}

// runForeverLoop checks the Forever SavedVariables every minute and uploads when it
// changed, or when the last pass needs a retry. The file is looked up again each time,
// so playing Forever for the first time while the uploader runs is picked up.
func runForeverLoop(apiBase string, dryRun, verbose bool) {
	var lastPath string
	var lastMod time.Time
	var lastSize int64
	for {
		if p := detectForeverSavedVariablesPath(); p != "" {
			if st, err := os.Stat(p); err == nil && (p != lastPath || !st.ModTime().Equal(lastMod) || st.Size() != lastSize) {
				if p != lastPath {
					log.Printf("forever: watching %s", p)
				}
				if runForeverOnce(p, apiBase, dryRun, verbose) {
					lastPath, lastMod, lastSize = p, st.ModTime(), st.Size()
				}
			}
		}
		time.Sleep(foreverScanInterval)
	}
}
