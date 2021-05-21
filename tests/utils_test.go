package tests

import (
	"os"
	"testing"

	"github.com/google/uuid"

	kodo "github.com/beyondstorage/go-service-kodo/v2"
	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/types"
)

func setupTest(t *testing.T) types.Storager {
	t.Log("Setup test for kodo")

	store, err := kodo.NewStorager(
		ps.WithCredential(os.Getenv("STORAGE_KODO_CREDENTIAL")),
		ps.WithName(os.Getenv("STORAGE_KODO_NAME")),
		ps.WithWorkDir("/"+uuid.New().String()+"/"),
		ps.WithEndpoint(os.Getenv("STORAGE_KODO_ENDPOINT")),
	)
	if err != nil {
		t.Errorf("new storager: %v", err)
	}
	return store
}
