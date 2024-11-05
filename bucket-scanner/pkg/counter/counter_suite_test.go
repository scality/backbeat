package counter_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCounter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Counter Suite")
}
