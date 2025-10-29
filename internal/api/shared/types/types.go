package types

// Order enumeration for sorting
type Order string

const (
	OrderAsc  Order = "asc"
	OrderDesc Order = "desc"
)

func (o Order) Desc() bool {
	return o == OrderDesc
}

func (o Order) Asc() bool {
	return o == OrderAsc
}

// Valid checks if an order is valid
func (o Order) Valid() bool {
	return o == OrderAsc || o == OrderDesc
}

// Expansion enumeration for expansions
type Expansion string

const (
	ExpansionOwners           Expansion = "owners"
	ExpansionProvenanceEvents Expansion = "provenance_events"
	ExpansionEnrichmentSource Expansion = "enrichment_source"
	ExpansionSubject          Expansion = "subject"
)

// Valid checks if an expansion is valid
func (e Expansion) Valid() bool {
	return e == ExpansionOwners ||
		e == ExpansionProvenanceEvents ||
		e == ExpansionEnrichmentSource ||
		e == ExpansionSubject
}
