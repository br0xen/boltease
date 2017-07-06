package boltease

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

// This is a library for easing the use of bolt dbs

// DB is a struct for accomplishing this
type DB struct {
	filename string
	boltDB   *bolt.DB
	mode     os.FileMode
	options  *bolt.Options
	dbIsOpen bool
}

// Create makes sure we can get open the file and returns the DB object
func Create(fn string, m os.FileMode, opts *bolt.Options) (*DB, error) {
	var err error
	b := DB{filename: fn, mode: m, options: opts}
	b.boltDB, err = bolt.Open(fn, m, opts)
	if err != nil {
		return nil, err
	}
	defer b.boltDB.Close()
	return &b, nil
}

func (b *DB) OpenDB() error {
	if b.dbIsOpen {
		// DB is already open, that's fine.
		return nil
	}
	var err error
	if b.boltDB, err = bolt.Open(b.filename, b.mode, b.options); err != nil {
		return err
	}
	b.dbIsOpen = true
	return err
}

func (b *DB) CloseDB() error {
	if !b.dbIsOpen {
		// DB is already closed, that's fine.
		return nil
	}
	var err error
	if err = b.boltDB.Close(); err != nil {
		return err
	}
	b.dbIsOpen = false
	return err
}

// MkBucketPath builds all buckets in the string slice
func (b *DB) MkBucketPath(path []string) error {
	var err error
	if !b.dbIsOpen {
		if err = b.OpenDB(); err != nil {
			return err
		}
		defer b.CloseDB()
	}

	err = b.boltDB.Update(func(tx *bolt.Tx) error {
		var err error
		bkt := tx.Bucket([]byte(path[0]))
		if bkt == nil {
			// Create it
			bkt, err = tx.CreateBucket([]byte(path[0]))
			if err != nil {
				// error creating
				return err
			}
		}
		if len(path) > 1 {
			path = path[1:]
			for i := range path {
				nextBkt := bkt.Bucket([]byte(path[i]))
				if nextBkt == nil {
					// Create it
					nextBkt, err = bkt.CreateBucket([]byte(path[i]))
					if err != nil {
						return err
					}
				}
				bkt = nextBkt
			}
		}
		return err
	})
	return err
}

// GetValue returns the value at path
// path is a slice of strings
// key is the key to get
func (b *DB) GetValue(path []string, key string) (string, error) {
	var err error
	var ret string
	if !b.dbIsOpen {
		if err = b.OpenDB(); err != nil {
			return ret, err
		}
		defer b.CloseDB()
	}
	err = b.boltDB.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(path[0]))
		if bkt == nil {
			return fmt.Errorf("Couldn't find bucket " + path[0])
		}
		for idx := 1; idx < len(path); idx++ {
			bkt = bkt.Bucket([]byte(path[idx]))
			if bkt == nil {
				return fmt.Errorf("Couldn't find bucket " + strings.Join(path[:idx], "/"))
			}
		}
		// newBkt should have the last bucket in the path
		ret = string(bkt.Get([]byte(key)))
		return nil
	})
	return ret, err
}

// SetValue sets the value of key at path to val
// path is a slice of tokens
func (b *DB) SetValue(path []string, key, val string) error {
	var err error
	if !b.dbIsOpen {
		if err = b.OpenDB(); err != nil {
			return err
		}
		defer b.CloseDB()
	}

	err = b.MkBucketPath(path)
	if err != nil {
		return err
	}
	err = b.boltDB.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(path[0]))
		if bkt == nil {
			return fmt.Errorf("Couldn't find bucket " + path[0])
		}
		for idx := 1; idx < len(path); idx++ {
			bkt, err = bkt.CreateBucketIfNotExists([]byte(path[idx]))
			if err != nil {
				return err
			}
		}
		// bkt should have the last bucket in the path
		return bkt.Put([]byte(key), []byte(val))
	})
	return err
}

// GetInt returns the value at path
// If the value cannot be parsed as an int, error
func (b *DB) GetInt(path []string, key string) (int, error) {
	var ret int
	r, err := b.GetValue(path, key)
	if err == nil {
		ret, err = strconv.Atoi(r)
	}
	return ret, err
}

// SetInt Sets an integer value
func (b *DB) SetInt(path []string, key string, val int) error {
	return b.SetValue(path, key, strconv.Itoa(val))
}

// GetBool returns the value at 'path'
// If the value cannot be parsed as a bool, error
// We check 'true/false' and '1/0', else error
func (b *DB) GetBool(path []string, key string) (bool, error) {
	var ret bool
	r, err := b.GetValue(path, key)
	if err == nil {
		if r == "true" || r == "1" {
			ret = true
		} else if r != "false" && r != "0" {
			err = fmt.Errorf("Cannot parse as a boolean")
		}
	}
	return ret, err
}

// SetBool Sets a boolean value
func (b *DB) SetBool(path []string, key string, val bool) error {
	if val {
		return b.SetValue(path, key, "true")
	}
	return b.SetValue(path, key, "false")
}

// GetTimestamp returns the value at 'path'
// If the value cannot be parsed as a RFC3339, error
func (b *DB) GetTimestamp(path []string, key string) (time.Time, error) {
	r, err := b.GetValue(path, key)
	if err == nil {
		return time.Parse(time.RFC3339, r)
	}
	return time.Unix(0, 0), err
}

// SetTimestamp saves a timestamp into the db
func (b *DB) SetTimestamp(path []string, key string, val time.Time) error {
	return b.SetValue(path, key, val.Format(time.RFC3339))
}

// GetBucketList returns a list of all sub-buckets at path
func (b *DB) GetBucketList(path []string) ([]string, error) {
	var err error
	var ret []string
	if !b.dbIsOpen {
		if err = b.OpenDB(); err != nil {
			return ret, err
		}
		defer b.CloseDB()
	}

	err = b.boltDB.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(path[0]))
		if bkt == nil {
			return fmt.Errorf("Couldn't find bucket " + path[0])
		}
		var berr error
		if len(path) > 1 {
			for idx := 1; idx < len(path); idx++ {
				bkt = bkt.Bucket([]byte(path[idx]))
				if bkt == nil {
					return fmt.Errorf("Couldn't find bucket " + strings.Join(path[:idx], " / "))
				}
			}
		}

		// bkt should have the last bucket in the path
		berr = bkt.ForEach(func(k, v []byte) error {
			if v == nil {
				// Must be a bucket
				ret = append(ret, string(k))
			}
			return nil
		})
		return berr
	})
	return ret, err
}

// GetKeyList returns a list of all keys at path
func (b *DB) GetKeyList(path []string) ([]string, error) {
	var err error
	var ret []string
	if !b.dbIsOpen {
		if err = b.OpenDB(); err != nil {
			return ret, err
		}
		defer b.CloseDB()
	}

	err = b.boltDB.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(path[0]))
		if bkt == nil {
			return fmt.Errorf("Couldn't find bucket " + path[0])
		}
		var berr error
		if len(path) > 1 {
			for idx := 1; idx < len(path); idx++ {
				bkt = bkt.Bucket([]byte(path[idx]))
				if bkt == nil {
					return fmt.Errorf("Couldn't find bucket " + strings.Join(path[:idx], " / "))
				}
			}
		}

		// bkt should have the last bucket in the path
		berr = bkt.ForEach(func(k, v []byte) error {
			if v != nil {
				// Must be a key
				ret = append(ret, string(k))
			}
			return nil
		})
		return berr
	})
	return ret, err
}

// DeletePair deletes the pair with key at path
func (b *DB) DeletePair(path []string, key string) error {
	var err error
	if !b.dbIsOpen {
		if err = b.OpenDB(); err != nil {
			return err
		}
		defer b.CloseDB()
	}

	err = b.boltDB.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(path[0]))
		if bkt == nil {
			return fmt.Errorf("Couldn't find bucket " + path[0])
		}
		if len(path) > 1 {
			var newBkt *bolt.Bucket
			for idx := 1; idx < len(path); idx++ {
				newBkt = bkt.Bucket([]byte(path[idx]))
				if newBkt == nil {
					return fmt.Errorf("Couldn't find bucket " + strings.Join(path[:idx], "/"))
				}
			}
			bkt = newBkt
		}
		// bkt should have the last bucket in the path
		// Test to make sure that key is a pair, if so, delete it
		if tst := bkt.Bucket([]byte(key)); tst == nil {
			return bkt.Delete([]byte(key))
		}
		return nil
	})
	return err
}

// DeleteBucket deletes the bucket key at path
func (b *DB) DeleteBucket(path []string, key string) error {
	var err error
	if !b.dbIsOpen {
		if err = b.OpenDB(); err != nil {
			return err
		}
		defer b.CloseDB()
	}

	err = b.boltDB.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(path[0]))
		if bkt == nil {
			return fmt.Errorf("Couldn't find bucket " + path[0])
		}
		for idx := 1; idx < len(path); idx++ {
			bkt = bkt.Bucket([]byte(path[idx]))
			if bkt == nil {
				return fmt.Errorf("Couldn't find bucket " + strings.Join(path[:idx], "/"))
			}
		}
		// bkt should have the last bucket in the path
		// Test to make sure that key is a bucket, if so, delete it
		if tst := bkt.Bucket([]byte(key)); tst != nil {
			return bkt.DeleteBucket([]byte(key))
		}
		return nil
	})
	return err
}
