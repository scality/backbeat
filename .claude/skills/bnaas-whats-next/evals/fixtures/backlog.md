# Backlog

The D1 work as a graph. Seeded from the breakdown on 2026-10-22; edit here, and what's next reads it. Depends on lists task ids. Origin is breakdown or new scope.

| ID | Task | Area | Repository | Estimate (days) | Depends on | Origin | Notes | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Release line decided: latest development branches, no backport | Specs merged and epic branches ready | federation | 0 |  | breakdown |  |  |
| 2 | Create the D1 Jira epic and component tickets | Specs merged and epic branches ready | jira | 0.5 |  | breakdown |  | done |
| 3 | Set up the epic branches and Integration CI against the Federation epic | Specs merged and epic branches ready | arsenal, vault, cloudserver, cloudserverclient, backbeat, federation, ring, integration, s3utils | 0.5 | 1, 2 | breakdown |  |  |
| 3.3a | Build the scality-kms image on every CloudServer epic build | Specs merged and epic branches ready | cloudserver | 0.5 | 3 | new scope | Dropped on 2026-10-22: no epic releases |  |
| 5 | Write the D1 cross-repo tech spec as an artifact | Specs merged and epic branches ready | artifact | 5 | 1 | breakdown |  |  |
| 6 | Settle the open decisions in the D1 tech spec artifact | Specs merged and epic branches ready | artifact | 1 | 5 | breakdown |  |  |
| 7 | Link the agreed D1 to D4 plan from the spec and file the follow-up tickets | Specs merged and epic branches ready | artifact | 0.25 | 5 | breakdown |  |  |
| 11 | Build a local multi-service dev rig | Specs merged and epic branches ready | workspace | 2 | 5 | breakdown |  |  |
| 12 | Review and approve the D1 tech spec artifact with Taylor | Specs merged and epic branches ready | artifact | 2 | 6, 18 | breakdown |  |  |
| 13 | Provision an S3C lab for the whole epic | Specs merged and epic branches ready | lab | 0.5 | 1 | breakdown |  | done |
| 14 | Write the Arsenal implementation spec as an artifact | Specs merged and epic branches ready | artifact, arsenal | 0.5 | 12 | breakdown |  |  |
| 15 | Write the CloudServer implementation spec as an artifact | Specs merged and epic branches ready | artifact, cloudserver | 1 | 12 | breakdown |  |  |
| 16 | Write the backbeat implementation spec as an artifact | Specs merged and epic branches ready | artifact, backbeat | 1.5 | 12 | breakdown |  |  |
| 17 | Write the Federation implementation spec as an artifact | Specs merged and epic branches ready | artifact, federation | 1 | 12 | breakdown |  |  |
| 18 | Build the bnaas-spec-review skill | Review skills and contract checks in place | workspace | 1 |  | breakdown |  |  |
| 19 | Build the bnaas-contract-check skill | Review skills and contract checks in place | workspace | 0.5 | 12 | breakdown |  |  |
| 20 | Build the bnaas-lab-run skill | Review skills and contract checks in place | workspace | 0.5 | 12 | breakdown |  |  |
| 11a | Build the bnaas-local-rig skill | Review skills and contract checks in place | workspace | 1 | 11 | breakdown |  |  |
| 20a | Give CI reviews access to the approved spec artifacts | Review skills and contract checks in place | scality-workflows | 1 | 5 | breakdown |  |  |
| 21 | Build the BNaaS review-pr skill for each repo | Review skills and contract checks in place | arsenal, vault, cloudserver, cloudserverclient, backbeat, federation, ring, integration, s3utils | 1 | 12, 20a | breakdown |  |  |
| 22 | Validate the review skills against planted defects | Review skills and contract checks in place | arsenal, vault, cloudserver, cloudserverclient, backbeat, federation, ring, integration, s3utils | 0.5 | 18, 19, 20, 21 | breakdown |  |  |
| 22a | Build the shared .claude plumbing (skills and Stop hooks) for every epic repo | Review skills and contract checks in place | arsenal, vault, cloudserver, cloudserverclient, backbeat, federation, ring, integration, s3utils | 2 | 3 | breakdown |  |  |
| 22b | What's next skill and PR review tracking | Review skills and contract checks in place | backbeat | 2 | 22a | new scope |  |  |
| 22c | Release skill and epic tag guard | Review skills and contract checks in place | backbeat | 1 | 22a | new scope | Dropped on 2026-10-22: no epic releases |  |
| 22d | Self-improvement PRs of the D1 skills, standing task | Review skills and contract checks in place | backbeat | 0 | 22a | new scope |  |  |
| 23 | Enforce the metrics contract in CI | Review skills and contract checks in place | backbeat, ring | 1 | 19 | breakdown |  |  |
| 24 | Route service-root ?notification-destination requests | IAM actions and API routing released, with IAM tests (Arsenal, Vault) | arsenal | 0.5 | 14 | breakdown |  |  |
| 25 | Add the bucketnotif service, five IAM actions and resource ARN | IAM actions and API routing released, with IAM tests (Arsenal, Vault) | arsenal | 0.75 | 14 | breakdown |  |  |
| 26 | Add the five named destination API errors | IAM actions and API routing released, with IAM tests (Arsenal, Vault) | arsenal | 0.5 | 14 | breakdown |  |  |
| 27 | Export an ARN parser and the destination name grammar | IAM actions and API routing released, with IAM tests (Arsenal, Vault) | arsenal | 0.5 | 14 | breakdown |  |  |
| 28 | Release Arsenal from its development branch for CloudServer, backbeat and Vault | IAM actions and API routing released, with IAM tests (Arsenal, Vault) | arsenal | 0.5 | 24, 25, 26, 27 | breakdown |  |  |
| 29 | Bump Arsenal in Vault and add an IAM-user policy test | IAM actions and API routing released, with IAM tests (Arsenal, Vault) | vault | 1 | 28 | breakdown |  |  |
| 30 | Pin the Vault epic image in Federation (automatic) | IAM actions and API routing released, with IAM tests (Arsenal, Vault) | vault, federation, cloudserver | 0.5 | 29 | breakdown |  |  |
| 31 | Bump Arsenal in CloudServer and build bucketnotif request contexts | IAM actions and API routing released, with IAM tests (Arsenal, Vault) | cloudserver | 0.5 | 28 | breakdown |  |  |
| 32 | TDD: Add Integration IAM tests for the five scality: actions | IAM actions and API routing released, with IAM tests (Arsenal, Vault) | integration | 1 | 14 | breakdown |  |  |
| 41 | TDD: Write CloudServer functional tests first, from the spec | S3 destination API: create, list, get, update, delete | cloudserver | 4 | 15, 11 | breakdown |  |  |
| 33 | Add the BNaaS feature flag and configuration parser | S3 destination API: create, list, get, update, delete | cloudserver | 0.5 | 15 | breakdown |  |  |
| 34 | Add a record store client for the dedicated bucketd cluster | S3 destination API: create, list, get, update, delete | cloudserver | 2 | 15 | breakdown |  |  |
| 37 | Implement the five destination API handlers | S3 destination API: create, list, get, update, delete | cloudserver | 3 | 34, 31, 41 | breakdown |  |  |
| 38 | Audit-log the destination API and the record route (approach open) | S3 destination API: create, list, get, update, delete | cloudserver | 1 | 37, 43 | breakdown |  |  |
| 39 | Add a typed client for the destination API | S3 destination API: create, list, get, update, delete | cloudserverclient | 2 | 5 | breakdown |  |  |
| 40 | Wire CloudServer CI for a second metadata cluster | S3 destination API: create, list, get, update, delete | cloudserver | 1 | 34 | breakdown |  |  |
| 42 | Gate PutBucketNotificationConfiguration on tenant destination ownership | Buckets can use tenant destinations (attach gate) | cloudserver | 2 | 37, 41 | breakdown |  |  |
| 43 | Serve decrypted destination records to workers on the internal port | Workers can read destination records (internal route) | cloudserver | 2 | 34, 41 | breakdown |  |  |
| 44 | Scaffold the new BNaaS backbeat extension | Tenant events stamped and published (populator) | backbeat | 1 | 16 | breakdown |  |  |
| 45 | Implement the tenant pass in the new extension's populator | Tenant events stamped and published (populator) | backbeat | 3 | 44, 59 | breakdown |  |  |
| 46 | Decide the populator placement: separate process or shared | Tenant events stamped and published (populator) | backbeat, federation | 0.5 | 48 | breakdown |  |  |
| 47 | Run the tenant populator as its own process | Tenant events stamped and published (populator) | backbeat | 1 | 45 | breakdown |  |  |
| 48 | Measure populator cost and the extra raft log reader | Tenant events stamped and published (populator) | lab | 1.5 | 47, 13 | breakdown |  |  |
| 59 | TDD: Write backbeat unit and functional tests first, against a real Kafka | Delivery to tenant destinations, with metrics and alerts (worker) | backbeat | 4 | 44, 11 | breakdown |  |  |
| 49 | Add an opt-in single-topic metadata option to BackbeatConsumer | Delivery to tenant destinations, with metrics and alerts (worker) | backbeat | 1 | 16 | breakdown |  |  |
| 50 | Implement the worker in the new extension: consume auto-0 and deliver by stamped ARN | Delivery to tenant destinations, with metrics and alerts (worker) | backbeat | 3 | 44, 49, 59 | breakdown |  |  |
| 51 | Cache destination records in the worker | Delivery to tenant destinations, with metrics and alerts (worker) | backbeat | 3 | 52 | breakdown |  |  |
| 52 | Fetch destination records from the internal route with SigV4 | Delivery to tenant destinations, with metrics and alerts (worker) | backbeat | 0.5 | 50 | breakdown |  |  |
| 53 | Refetch a destination record on authentication errors | Delivery to tenant destinations, with metrics and alerts (worker) | backbeat | 1.5 | 51 | breakdown |  |  |
| 53a | Evaluate and choose the Kafka client library for tenant producers | Delivery to tenant destinations, with metrics and alerts (worker) | backbeat | 2 | 16 | breakdown |  |  |
| 54 | Create a producer per destination with inline credentials and an address check | Delivery to tenant destinations, with metrics and alerts (worker) | backbeat | 4 | 53a, 50 | breakdown |  |  |
| 55 | Cap producer buffers and commit only on terminal resolution | Delivery to tenant destinations, with metrics and alerts (worker) | backbeat | 1.5 | 54, 53a | breakdown |  |  |
| 56 | Add the per-destination retry budget and breaker | Delivery to tenant destinations, with metrics and alerts (worker) | backbeat | 2.5 | 54 | breakdown |  |  |
| 57 | Expose worker metrics, a health endpoint and a watchdog | Delivery to tenant destinations, with metrics and alerts (worker) | backbeat | 2 | 50 | breakdown |  |  |
| 58 | Add guard rails on the record cache TTL | Delivery to tenant destinations, with metrics and alerts (worker) | backbeat | 0.5 | 50 | breakdown |  |  |
| 60 | Prove legacy notification, replication and lifecycle are unchanged | Delivery to tenant destinations, with metrics and alerts (worker) | backbeat | 1 | 59 | breakdown |  |  |
| 61 | Add S3C alert rules and a dashboard for tenant delivery | Delivery to tenant destinations, with metrics and alerts (worker) | ring | 2 | 57 | breakdown |  |  |
| 62 | Write the alert runbook pages | Delivery to tenant destinations, with metrics and alerts (worker) | ring | 1 | 61 | breakdown |  |  |
| 63 | Add the level-2 lifeline and the S3C health indicator | Delivery to tenant destinations, with metrics and alerts (worker) | ring | 1 | 57 | breakdown |  |  |
| 64 | Create the new Federation role and wire it into run.yml | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 1 | 17 | breakdown |  |  |
| 65 | Add group\_vars and defaults for the BNaaS setting | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 1.5 | 17 | breakdown |  |  |
| 66 | Deploy the dedicated bucketd cluster for destination records | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 5 | 17 | breakdown |  |  |
| 67 | Fix shared metadata roles for a new deploy target | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 1 | 66 | breakdown |  |  |
| 68 | Check port conflicts over the reserved port block | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 1 | 17 | breakdown |  |  |
| 70 | Provision ZooKeeper nodes and run the tenant populator program | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 1 | 47 | breakdown |  |  |
| 71 | Deploy the BNaaS worker container | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 2 | 64 | breakdown |  |  |
| 72 | Remove the BNaaS containers when the setting is off | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 0.5 | 71 | breakdown |  |  |
| 73 | Create the workers' service user | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 0.5 | 17 | breakdown |  |  |
| 74 | Create the auto-0 Kafka topic | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 1 | 17 | breakdown |  |  |
| 75 | Render CloudServer's BNaaS configuration | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 1 | 66 | breakdown |  |  |
| 76 | Roll the S3 and BNaaS plays one node at a time | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 1 | 17 | breakdown |  |  |
| 77 | Add logrotate and non-root for the record cluster and the workers | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 1 | 66, 71 | breakdown |  |  |
| 78 | Add Prometheus targets for the workers and the populator | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 0.5 | 71 | breakdown |  |  |
| 79 | Add one shared Ansible tag to every Federation play D1 touches | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 0.5 |  | breakdown | Dependencies unknown: All Federation tasks above |  |
| 80 | Teach the status and topology playbooks the new cluster | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 1 | 66 | breakdown |  |  |
| 81 | Update the client-template and light-local inventories | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 1 | 65, 66 | breakdown |  |  |
| 82 | Update Federation CLAUDE.md for the new role and variables | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | federation | 0.5 | 64 | breakdown |  |  |
| 83 | Sync generate\_s3\_inventory with the Federation client template | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | ring | 1 | 81 | breakdown |  |  |
| 83a | Update the s3c-upgrade tool for D1, if needed | Deployable on S3C, identical to today with switches off, with Prometheus targets (Federation) | ring | 1 | 64, 66, 81 | breakdown |  |  |
| 35 | Encrypt destination credentials with an envelope and load the keyfile | Encryption at rest (after the clear-text pass, gated before lab round 1) | cloudserver | 2 | 34, 15 | breakdown |  |  |
| 36 | Support dual-key read for future key rotation | Encryption at rest (after the clear-text pass, gated before lab round 1) | cloudserver | 0.5 | 35 | breakdown |  |  |
| 69 | Generate and distribute the master keyfile | Encryption at rest (after the clear-text pass, gated before lab round 1) | federation | 1 | 17 | breakdown |  |  |
| 75a | Mount the keyfile and set its path in the S3 container | Encryption at rest (after the clear-text pass, gated before lab round 1) | federation | 0.5 | 69, 75 | breakdown |  |  |
| 87a | Add the keyfile hash pre-check | Encryption at rest (after the clear-text pass, gated before lab round 1) | federation | 0.5 | 69, 87 | breakdown |  |  |
| 84 | Decide which repo holds the admin tools | Safe operations: pre-checks and admin tools | s3utils | 0.25 |  | breakdown |  |  |
| 85 | Build a bucket notification configuration scanner | Safe operations: pre-checks and admin tools | s3utils | 1 | 84 | breakdown |  |  |
| 86 | Build the admin name-check tool | Safe operations: pre-checks and admin tools | s3utils | 0.5 | 66, 37 | breakdown |  |  |
| 87 | Add the D1 Federation pre-checks | Safe operations: pre-checks and admin tools | federation | 2.5 | 30 | breakdown | Dependencies unknown: All Federation tasks above |  |
| 88 | Add the name-collision pre-check | Safe operations: pre-checks and admin tools | federation | 1 | 87, 86, 85 | breakdown |  |  |
| 89 | Build a tool that lists bucket notification configurations referencing tenant destinations | Safe operations: pre-checks and admin tools | s3utils | 0.5 | 85 | breakdown |  |  |
| 90 | TDD: Add end-to-end BNaaS tests in Integration | Verified together: end to end, lab rounds, performance | integration | 4 | 12, 3 | breakdown |  |  |
| 91 | Run the lab operations rounds twice | Verified together: end to end, lab rounds, performance | lab | 6 | 87, 90, 35, 83a | breakdown |  |  |
| 92 | Measure worker capacity and CloudServer record-fetch load | Verified together: end to end, lab rounds, performance | lab | 3 | 91 | breakdown |  |  |
| 93a | Fold BNaaS review learnings into each repo's default review-pr skill | Documented, released and merged | arsenal, vault, cloudserver, cloudserverclient, backbeat, federation, ring, integration | 0.5 | 21 | breakdown |  |  |
| 93 | Rebase every epic onto the latest development branch, re-pin and merge | Documented, released and merged | arsenal, vault, cloudserver, cloudserverclient, backbeat, federation, ring, integration, s3utils | 3.5 |  | breakdown | Dependencies unknown: All component tasks |  |
| 94 | Sign off the dependency and supply-chain scan | Documented, released and merged | arsenal, vault, cloudserver, cloudserverclient, backbeat, federation, ring, integration, s3utils | 0.5 |  | breakdown | Dependencies unknown: Code complete |  |
| 95 | Write the customer documentation | Documented, released and merged | documentation (techpub) | 2.5 | 41 | breakdown |  |  |
| 96 | Write the enablement, downgrade and switch-off runbooks | Documented, released and merged | documentation (techpub) | 1.5 | 91 | breakdown |  |  |
| 97 | Write the DR dual-create runbook | Documented, released and merged | documentation (techpub) | 0.5 | 37 | breakdown |  |  |
| 98 | Write the security documentation | Documented, released and merged | documentation (techpub) | 0.5 | 12 | breakdown |  |  |
| 99 | Provide release notes and techpub inputs | Documented, released and merged | jira, documentation (techpub) | 0.5 | 95 | breakdown |  |  |
| 100 | Record the D1 demo on a lab | Documented, released and merged | lab | 1 | 91 | breakdown |  |  |
| 101 | Show red then green CI evidence on every PR | Across all deliverables | arsenal, vault, cloudserver, cloudserverclient, backbeat, federation, ring, integration, s3utils | 0 |  | breakdown | Dependencies unknown: Each PR |  |
| 102 | Allow for review loops (person A) | Across all deliverables | arsenal, vault, cloudserver, cloudserverclient, backbeat, federation, ring, integration, s3utils | 11 |  | breakdown | Dependencies unknown: Each PR |  |
| 103 | Allow for review loops (person B) | Across all deliverables | arsenal, vault, cloudserver, cloudserverclient, backbeat, federation, ring, integration, s3utils | 11 |  | breakdown | Dependencies unknown: Each PR |  |
| 103a | Allow for review loops (Nicolas Humbert, third reviewer) | Across all deliverables | arsenal, vault, cloudserver, cloudserverclient, backbeat, federation, ring, integration, s3utils | 9 |  | breakdown | Dependencies unknown: Each PR |  |
