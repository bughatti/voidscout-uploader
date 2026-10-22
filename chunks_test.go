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
