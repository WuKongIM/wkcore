package wkdb

type ShardStore struct {
	db   *DB
	slot uint64
}

func validateSlot(slot uint64) error {
	if slot == 0 {
		return ErrInvalidArgument
	}
	return nil
}
