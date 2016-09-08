package integration_test

import (
	"io/ioutil"
	"net/http"

	"github.com/comail/go-uuid/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Create Topic", func() {
	Context("When topic already exists", func() {
		var topicName string
		var response *http.Response
		BeforeEach(func() {
			topicName = uuid.NewRandom().String()
			var err error
			response, err = http.Post("http://localhost:12345/"+topicName, "text/plain", nil)
			Expect(err).ToNot(HaveOccurred())
			err = response.Body.Close()
			Expect(err).ToNot(HaveOccurred())
			response, err = http.Post("http://localhost:12345/"+topicName, "text/plain", nil)
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			err := response.Body.Close()
			Expect(err).ToNot(HaveOccurred())
		})

		It("Should respond with 400 status code", func() {
			Expect(response.StatusCode).To(Equal(400))
		})

		It("Should have body confirming topic creation", func() {
			body, err := ioutil.ReadAll(response.Body)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(body)).To(MatchJSON(`{"ok":false,"status":400,"error":"netlog: topic exists"}`))
		})

		It("Should have application/json Content-Type", func() {
			Expect(response.Header.Get("Content-Type")).To(ContainSubstring("application/json"))
		})

	})

	Context("When topic does not exist", func() {
		var topicName string
		var response *http.Response
		BeforeEach(func() {
			topicName = uuid.NewRandom().String()
			var err error
			response, err = http.Post("http://localhost:12345/"+topicName, "text/plain", nil)
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			err := response.Body.Close()
			Expect(err).ToNot(HaveOccurred())
		})

		It("Should respond with 201 status code", func() {
			Expect(response.StatusCode).To(Equal(201))
		})

		It("Should have body confirming topic creation", func() {
			body, err := ioutil.ReadAll(response.Body)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(body)).To(MatchJSON(`{"ok":true,"message":"topic created"}`))
		})

		// XIt("Should have application/json Content-Type", func() {
		// 	Expect(response.Header.Get("Content-Type")).To(ContainSubstring("application/json"))
		// })

	})
})
