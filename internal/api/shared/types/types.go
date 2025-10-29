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

// Expansion enumeration for expansions
type Expansion string

const (
	ExpansionOwners           Expansion = "owners"
	ExpansionProvenanceEvents Expansion = "provenance_events"
	ExpansionEnrichmentSource Expansion = "enrichment_source"
	ExpansionSubject          Expansion = "subject"
)
