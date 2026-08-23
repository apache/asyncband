// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::convert::Infallible;
use std::sync::Arc;

use asyncband::pool::ManageObject;
use asyncband::pool::ObjectStatus;
use asyncband::pool::bounded::Pool;
use asyncband::pool::bounded::PoolConfig;

#[tokio::test]
async fn test_replenish() {
    #[derive(Default)]
    struct Manager;

    impl ManageObject for Manager {
        type Object = ();
        type Error = Infallible;

        async fn create(&self) -> Result<Self::Object, Self::Error> {
            Ok(())
        }

        async fn is_recyclable(
            &self,
            _o: &mut Self::Object,
            _status: &ObjectStatus,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    const MAX_SIZE: usize = 2;

    fn make_default() -> Arc<Pool<Manager>> {
        Pool::new(PoolConfig::new(MAX_SIZE), Manager)
    }

    for i in 0..5 {
        let pool = make_default();
        let n = pool.replenish(i).await;
        assert_eq!(n, i.min(MAX_SIZE));
    }

    // stage one idle object
    {
        let pool = make_default();
        pool.get().await.unwrap();
        let n = pool.replenish(2).await;
        assert_eq!(n, 1);
    }

    // stage two idle objects
    {
        let pool = make_default();
        let o1 = pool.get().await.unwrap();
        let o2 = pool.get().await.unwrap();
        drop((o1, o2));

        let n = pool.replenish(2).await;
        assert_eq!(n, 0);
    }
}
