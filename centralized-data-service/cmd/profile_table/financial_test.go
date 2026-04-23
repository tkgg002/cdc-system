package main

import "testing"

func TestIsFinancialField_Positive(t *testing.T) {
	cases := []string{
		"amount",
		"total_amount",
		"refund_amount",
		"user_balance",
		"transaction_id", // matches 'transaction' prefix
		"payment_method",
		"currency",
		"price_usd",
		"fee",
		"debit_note",
		"credit_card",
		"charge_id",
		"deposit_amount", // matches both prefix+suffix
		"withdraw",
		"transfer_ref",
		"settlement_date",
		"final_price",
		"service_fee",
		"grand_total",
		"line_sum",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			if !IsFinancialField(name) {
				t.Fatalf("expected %q to be detected as financial", name)
			}
		})
	}
}

func TestIsFinancialField_Negative(t *testing.T) {
	cases := []string{
		"created_at",
		"updated_at",
		"status",
		"description",
		"user_id",
		"email",
		"phone",
		"address",
		"merchant_name",
		"country_code",
		"note",
		"tags",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			if IsFinancialField(name) {
				t.Fatalf("expected %q to NOT be detected as financial", name)
			}
		})
	}
}
