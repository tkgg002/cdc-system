package config

import (
	"reflect"
	"strings"
	"testing"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

// Phase multi_engine_unified — T1.1 DoD.
// Two YAML forms must decode to []string:
//   1) scalar (backward-compat) — `topicPrefix: cdc.gpay`
//   2) list                     — `topicPrefix: [cdc.gpay, cdc.goopay]`
// Plus the alias `topicPrefixes:` must be unioned in.

func decodeKafka(t *testing.T, yamlBody string) KafkaConfig {
	t.Helper()
	v := viper.New()
	v.SetConfigType("yml")
	if err := v.ReadConfig(strings.NewReader(yamlBody)); err != nil {
		t.Fatalf("read yaml: %v", err)
	}
	cfg := &AppConfig{}
	hook := viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		stringToStringSliceHookFunc(),
	))
	if err := v.Unmarshal(cfg, hook); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	mergeTopicPrefixAlias(v, cfg)
	return cfg.Kafka
}

func TestUnmarshalKafka_ScalarTopicPrefix(t *testing.T) {
	got := decodeKafka(t, `
kafka:
  topicPrefix: cdc.gpay
`)
	want := []string{"cdc.gpay"}
	if !reflect.DeepEqual(got.TopicPrefix, want) {
		t.Fatalf("got %#v, want %#v", got.TopicPrefix, want)
	}
}

func TestUnmarshalKafka_ListTopicPrefix(t *testing.T) {
	got := decodeKafka(t, `
kafka:
  topicPrefix:
    - cdc.gpay
    - cdc.goopay
    - cdc.mariadb
`)
	want := []string{"cdc.gpay", "cdc.goopay", "cdc.mariadb"}
	if !reflect.DeepEqual(got.TopicPrefix, want) {
		t.Fatalf("got %#v, want %#v", got.TopicPrefix, want)
	}
}

func TestUnmarshalKafka_AliasTopicPrefixes(t *testing.T) {
	got := decodeKafka(t, `
kafka:
  topicPrefixes:
    - cdc.gpay
    - cdc.goopay
`)
	want := []string{"cdc.gpay", "cdc.goopay"}
	if !reflect.DeepEqual(got.TopicPrefix, want) {
		t.Fatalf("got %#v, want %#v", got.TopicPrefix, want)
	}
}

func TestUnmarshalKafka_AliasUnionDedup(t *testing.T) {
	got := decodeKafka(t, `
kafka:
  topicPrefix:
    - cdc.gpay
  topicPrefixes:
    - cdc.gpay      # dup, must be deduped
    - cdc.mariadb
`)
	want := []string{"cdc.gpay", "cdc.mariadb"}
	if !reflect.DeepEqual(got.TopicPrefix, want) {
		t.Fatalf("got %#v, want %#v", got.TopicPrefix, want)
	}
}
