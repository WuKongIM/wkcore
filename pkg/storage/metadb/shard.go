package metadb

type ShardStore struct {
	db   *DB
	slot uint64
}

func (s *ShardStore) validate() error {
	if s == nil || s.db == nil {
		return ErrInvalidArgument
	}
	return validateSlot(s.slot)
}

func validateSlot(slot uint64) error {
	if slot == 0 {
		return ErrInvalidArgument
	}
	return nil
}
