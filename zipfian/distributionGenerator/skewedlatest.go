// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package distributionGenerator

import (
	"github.com/taosdata/tsbs/zipfian/ycsb"
	"math/rand"
	"time"
)

// SkewedLatest generates a popularity distribution of items,
// skewed to favor recent items significantly more than older items.
type SkewedLatest struct {
	Number
	basis   ycsb.Generator
	zipfian *Zipfian
}

// NewSkewedLatest creates the SkewedLatest counter.
// basis is Counter or AcknowledgedCounter
func NewSkewedLatest(basis ycsb.Generator) *SkewedLatest {
	zipfian := NewZipfianWithItems(basis.Last(), ZipfianConstant)
	s := &SkewedLatest{
		basis:   basis,
		zipfian: zipfian,
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	s.Next(r)
	return s
}

// Next implements the Generator Next interface.
func (s *SkewedLatest) Next(r *rand.Rand) int64 {
	max := s.basis.Last()
	next := max - s.zipfian.next(r, max)
	s.SetLastValue(next)
	return next
}
