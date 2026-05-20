package handlers

import "testing"

func TestParseListenerNotification_LegacyGlobalUID(t *testing.T) {
	got := parseListenerNotification("cluster-a:resource-uid")

	if got.GlobalUID != "cluster-a:resource-uid" {
		t.Fatalf("GlobalUID = %q, want %q", got.GlobalUID, "cluster-a:resource-uid")
	}
	if got.EventID != "" {
		t.Fatalf("EventID = %q, want empty", got.EventID)
	}
}

func TestParseListenerNotification_EventIDPayload(t *testing.T) {
	got := parseListenerNotification(`{"event_id":"event-123","global_uid":"cluster-a:resource-uid"}`)

	if got.EventID != "event-123" {
		t.Fatalf("EventID = %q, want %q", got.EventID, "event-123")
	}
	if got.GlobalUID != "cluster-a:resource-uid" {
		t.Fatalf("GlobalUID = %q, want %q", got.GlobalUID, "cluster-a:resource-uid")
	}
}

func TestParseListenerNotification_JSONWithoutEventIDFallsBackToGlobalUID(t *testing.T) {
	got := parseListenerNotification(`{"global_uid":"cluster-a:resource-uid"}`)

	if got.GlobalUID != "cluster-a:resource-uid" {
		t.Fatalf("GlobalUID = %q, want %q", got.GlobalUID, "cluster-a:resource-uid")
	}
	if got.EventID != "" {
		t.Fatalf("EventID = %q, want empty", got.EventID)
	}
}
