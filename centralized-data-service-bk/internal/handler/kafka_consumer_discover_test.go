package handler

import (
	"reflect"
	"sort"
	"testing"
)

// Phase multi_engine_unified — T1.2 DoD.
// Multi-prefix discovery returns the UNION of matches and dedupes
// when two engines happen to share an object name.

func TestKafkaConsumerDiscover_UnionThreePrefixes(t *testing.T) {
	topics := []string{
		"cdc.gpay.public.orders",
		"cdc.gpay.public.payments",
		"cdc.goopay.payment-bill-service.payment-bills",
		"cdc.goopay.payment-bill-service.refund-requests",
		"cdc.mariadb.goopay_legacy_maria.legacy_orders",
		"unrelated.topic",
		"_consumer_offsets",
	}
	prefixes := []string{"cdc.gpay", "cdc.goopay", "cdc.mariadb"}
	got, perPrefix, _ := filterMatchingTopics(topics, prefixes, nil)

	want := []string{
		"cdc.gpay.public.orders",
		"cdc.gpay.public.payments",
		"cdc.goopay.payment-bill-service.payment-bills",
		"cdc.goopay.payment-bill-service.refund-requests",
		"cdc.mariadb.goopay_legacy_maria.legacy_orders",
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("union got %v, want %v", got, want)
	}
	if perPrefix["cdc.gpay"] != 2 ||
		perPrefix["cdc.goopay"] != 2 ||
		perPrefix["cdc.mariadb"] != 1 {
		t.Fatalf("per-prefix count mismatch: %v", perPrefix)
	}
}

func TestKafkaConsumerDiscover_NoCollisionWhenSameObjectName(t *testing.T) {
	// PG `orders` and Mongo `orders` collection — same name, different
	// engines. Each comes through its own prefix; the topic strings
	// differ, so the union must contain BOTH (no de-dupe across engines).
	topics := []string{
		"cdc.gpay.public.orders",
		"cdc.goopay.payment-bill-service.orders",
	}
	prefixes := []string{"cdc.gpay", "cdc.goopay"}
	got, _, _ := filterMatchingTopics(topics, prefixes, nil)

	if len(got) != 2 {
		t.Fatalf("expected 2 topics across engines, got %v", got)
	}
}

func TestKafkaConsumerDiscover_RegistryFilterPermissiveOnEmpty(t *testing.T) {
	// Empty debeziumTables ⇒ no filter (matches legacy behaviour).
	topics := []string{"cdc.gpay.public.orders"}
	got, _, _ := filterMatchingTopics(topics, []string{"cdc.gpay"}, map[string]bool{})
	if len(got) != 1 {
		t.Fatalf("permissive filter expected 1 topic, got %v", got)
	}
}

func TestKafkaConsumerDiscover_RegistryFilterAppliedWhenSet(t *testing.T) {
	// When debeziumTables has entries, only matching object names pass.
	topics := []string{
		"cdc.gpay.public.orders",
		"cdc.gpay.public.users",
	}
	allow := map[string]bool{"orders": true}
	got, _, _ := filterMatchingTopics(topics, []string{"cdc.gpay"}, allow)
	want := []string{"cdc.gpay.public.orders"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("filtered got %v, want %v", got, want)
	}
}

func TestKafkaConsumerDiscover_BlankPrefixesIgnored(t *testing.T) {
	topics := []string{"cdc.gpay.public.orders"}
	_, _, prefixes := filterMatchingTopics(topics, []string{"  ", "", "cdc.gpay"}, nil)
	if !reflect.DeepEqual(prefixes, []string{"cdc.gpay"}) {
		t.Fatalf("expected blanks dropped, got %v", prefixes)
	}
}
