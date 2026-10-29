package main

import (
	"bytes"
	"testing"
)

func testLog() ([]byte, int64) {
	var b bytes.Buffer
	b.WriteString("10/22/2026 00:00:00.000-6  COMBAT_LOG_VERSION,22\n")
	b.WriteString("10/22/2026 00:00:01.000-6  ZONE_CHANGE,2993,\"Altar of Fangs\",8\n")
	b.WriteString("10/22/2026 00:30:20.000-6  ENCOUNTER_START,3456,\"Rav'i\",8,5,2993\n")
	b.WriteString("10/22/2026 00:30:21.000-6  SPELL_CAST_SUCCESS,x\n")
	b.WriteString("10/22/2026 00:31:41.000-6  ENCOUNTER_END,3456,\"Rav'i\",8,5,1,81520\n")
	firstEnd := int64(b.Len())
	b.WriteString("10/22/2026 00:34:42.000-6  ENCOUNTER_START,3457,\"The Writhing Coil\",8,5,2993\n")
	b.WriteString("10/22/2026 00:36:32.000-6  ENCOUNTER_END,3457,\"The Writhing Coil\",8,5,1,109761\n")
	return b.Bytes(), firstEnd
}

func TestChunksAfterSkipsAlreadySent(t *testing.T) {
	log, firstEnd := testLog()
	if n := len(extractEncounterChunks(log)); n != 2 {
		t.Fatalf("all chunks: got %d, want 2", n)
	}
	got := extractEncounterChunksAfter(log, firstEnd)
	if len(got) != 1 || !bytes.Contains(got[0], []byte("Writhing Coil")) {
		t.Fatalf("after first encounter: got %d chunks, want only Writhing Coil", len(got))
	}
	if !bytes.HasPrefix(got[0], []byte("10/22/2026 00:00:00.000-6  COMBAT_LOG_VERSION")) {
		t.Fatalf("chunk must keep the COMBAT_LOG_VERSION header")
	}
}

func TestChunksAfterKeepsStraddlingEncounter(t *testing.T) {
	log, firstEnd := testLog()
	// Last upload happened mid-Rav'i (before its END): Rav'i must still be sent.
	got := extractEncounterChunksAfter(log, firstEnd-10)
	if len(got) != 2 {
		t.Fatalf("straddling encounter: got %d chunks, want 2", len(got))
	}
}

// A key: trash, boss (with COMBATANT_INFO), trash, key end. Trash comes out as two chunks,
// each led by the header, the key start and the run's COMBATANT_INFO, and none of the
// boss's own lines.
func TestTrashChunks(t *testing.T) {
	var b bytes.Buffer
	b.WriteString("10/22/2026 00:00:00.000-6  COMBAT_LOG_VERSION,22\n")
	b.WriteString("10/22/2026 00:00:01.000-6  ZONE_CHANGE,2993,\"Altar of Fangs\",8\n")
	b.WriteString("10/22/2026 00:01:00.000-6  CHALLENGE_MODE_START,\"Altar of Fangs\",2993,555,7,[9]\n")
	b.WriteString("10/22/2026 00:01:05.000-6  SPELL_DAMAGE,trash1\n")
	b.WriteString("10/22/2026 00:02:00.000-6  ENCOUNTER_START,3456,\"Rav'i\",8,5,2993\n")
	b.WriteString("10/22/2026 00:02:00.100-6  COMBATANT_INFO,Player-1-AAAA,0,1,2,3\n")
	b.WriteString("10/22/2026 00:02:05.000-6  SPELL_DAMAGE,bosshit\n")
	b.WriteString("10/22/2026 00:03:00.000-6  ENCOUNTER_END,3456,\"Rav'i\",8,5,1,60000\n")
	b.WriteString("10/22/2026 00:03:10.000-6  SPELL_DAMAGE,trash2\n")
	b.WriteString("10/22/2026 00:04:00.000-6  CHALLENGE_MODE_END,2993,1,7,180000\n")
	got := extractTrashChunksAfter(b.Bytes(), 0)
	if len(got) != 2 {
		t.Fatalf("got %d trash chunks, want 2", len(got))
	}
	for i, c := range got {
		for _, want := range []string{"COMBAT_LOG_VERSION", "CHALLENGE_MODE_START", "COMBATANT_INFO,Player-1-AAAA"} {
			if !bytes.Contains(c, []byte(want)) {
				t.Fatalf("chunk %d lacks %s:\n%s", i, want, c)
			}
		}
		if bytes.Contains(c, []byte("bosshit")) || bytes.Contains(c, []byte("ENCOUNTER_START")) {
			t.Fatalf("chunk %d has boss lines:\n%s", i, c)
		}
	}
	if !bytes.Contains(got[0], []byte("trash1")) || !bytes.Contains(got[1], []byte("trash2")) {
		t.Fatal("trash lines in the wrong chunks")
	}
	if n := len(extractTrashChunksAfter(b.Bytes(), int64(b.Len()))); n != 0 {
		t.Fatalf("already-sent log: got %d chunks, want 0", n)
	}
}

// An oversize trash segment is cut after the line before a 6+ s lull, not mid-pull.
func TestSplitAtLull(t *testing.T) {
	var b bytes.Buffer
	for _, ts := range []string{"10:00:00.000", "10:00:01.000", "10:00:02.000", "10:00:10.000", "10:00:11.000"} {
		b.WriteString("10/22/2026 " + ts + "-6  SPELL_DAMAGE,x\n")
	}
	data := b.Bytes()
	cut := splitAtLull(data, 0, len(data))
	if !bytes.HasSuffix(data[:cut], []byte("10:00:02.000-6  SPELL_DAMAGE,x\n")) {
		t.Fatalf("cut after %q, want after the 10:00:02 line", data[:cut])
	}
}
