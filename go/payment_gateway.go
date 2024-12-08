package main

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/goccy/go-json"
	"github.com/oklog/ulid/v2"
)

var erroredUpstream = errors.New("errored upstream")

type paymentGatewayPostPaymentRequest struct {
	Amount int `json:"amount"`
}

type paymentGatewayGetPaymentsResponseOne struct {
	Amount int    `json:"amount"`
	Status string `json:"status"`
}

var paymentSem = make(chan struct{}, 100)

func requestPaymentGatewayPostPayment(paymentGatewayURL string, token string, param *paymentGatewayPostPaymentRequest, retrieveRidesOrderByCreatedAtAsc func() ([]Ride, error)) error {
	b, err := json.Marshal(param)
	if err != nil {
		return err
	}
	// 2つまでのリクエストを同時に送る
	paymentSem <- struct{}{}
	defer func() { <-paymentSem }()
	idempotencyKey := ulid.Make().String()

	// 失敗したらとりあえずリトライ
	// FIXME: 社内決済マイクロサービスのインフラに異常が発生していて、同時にたくさんリクエストすると変なことになる可能性あり
	retry := 0
	for {
		err := func() error {
			req, err := http.NewRequest(http.MethodPost, paymentGatewayURL+"/payments", bytes.NewBuffer(b))
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Idempotency-Key", idempotencyKey)

			res, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("failed to POST request to payment gateway: %w", err)
			}
			defer res.Body.Close()

			if res.StatusCode != http.StatusNoContent {
				return fmt.Errorf("failed to POST request to payment gateway: status code is not 204, got %d", res.StatusCode)
			}
			return nil
		}()
		if err != nil {
			if retry < 5 {
				retry++
				//time.Sleep(100 * time.Millisecond)
				continue
			} else {
				slog.Error("failed to request to payment gateway", "retry", retry, "err", err)
				return err
			}
		}
		break
	}

	return nil
}
