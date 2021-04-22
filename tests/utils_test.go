package tests

import (
	"os"
	"testing"

	"github.com/google/uuid"

	kodo "github.com/aos-dev/go-service-kodo"
	ps "github.com/aos-dev/go-storage/v3/pairs"
	"github.com/aos-dev/go-storage/v3/types"
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
