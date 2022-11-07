package npool

type Element interface {
	Close() error
}
