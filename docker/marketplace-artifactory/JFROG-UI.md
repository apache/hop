<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Surviving the JFrog Platform UI (Artifactory OSS 7.x)

The product UI is the **JFrog Platform** SPA at:

```text
http://localhost:8082/ui/
```

Older docs still say “Artifactory webapp” (`/artifactory/webapp/`) — that redirects into `/ui/`.
Menu labels move between minor releases; **deep links** below are more reliable than hunting the nav.

## Orientation (this is the frustrating part)

After login you may land in the **Application** module (packages, builds, scans).

Admin work is under **Administration**:

- Top bar: switch **Application | Administration**, **or**
- Gear / “Administration” control, **or**
- Left rail only shows admin sections once Administration is selected

If the left menu has no “Repositories” / “Security”, you are still in Application.

## Direct links (bookmark these)

| Task | Open |
|------|------|
| Repositories | http://localhost:8082/ui/admin/repositories |
| Anonymous access | http://localhost:8082/ui/admin/security/general |
| Permissions | http://localhost:8082/ui/admin/user_management/permissions |
| Users | http://localhost:8082/ui/admin/user_management/users |

(All return the SPA; the path selects the admin page after you are logged in.)

## A) Local Maven repository `hop-plugins-local`

1. Open **Repositories** link above (Administration module).
2. **+ Add Repositories** / **Create Repository** / **New**.
3. Type: **Local**.
4. Package type: **Maven**.
5. **Repository Key:** `hop-plugins-local` (exact spelling).
6. Layout: **maven-2-default**.
7. Create.

**OSS note:** REST `PUT /artifactory/api/repositories/...` returns 400 *Pro only*. UI is mandatory.

## B) Allow Anonymous Access

1. Open **Anonymous / Security General** link above.  
   Path in menu: **Administration → Security → General**  
   (Some builds still show a toggle under User Management → Settings — same setting.)
2. Enable **Allow Anonymous Access**.
3. **Save**.

## C) Permission: anonymous may **Read** only that repo

Since **7.84.3**, anonymous is **not** on the default “Anything” permission.

1. Open **Permissions** link above.  
   Menu: **Administration → User Management → Permissions**.
2. **+ New Permission**.
3. Name: `hop-plugins-anonymous-read`.
4. **Repositories** (or Resources): select **only** `hop-plugins-local`.  
   Do not select “Any Local” unless you want every local repo readable.
5. **Users**: add **anonymous**.
6. Actions for anonymous:

   | Action | Anonymous |
   |--------|-----------|
   | Read | ☑ |
   | Annotate | ☐ |
   | Deploy / Cache | ☐ |
   | Delete / Overwrite | ☐ |
   | Manage | ☐ |

7. Save.

## Verify (no UI archaeology)

```bash
export ARTIFACTORY_PASSWORD='your-admin-password'
./docker/marketplace-artifactory/configure-anonymous-read.sh
```

Or:

```bash
# Must not be 401
curl -sI "http://localhost:8082/artifactory/hop-plugins-local/" | head -1
```

Then Hop install **without** admin password:

```bash
unset HOP_MARKETPLACE_PASSWORD ARTIFACTORY_PASSWORD
# hop-config: repositories[0].url = http://localhost:8082/artifactory/hop-plugins-local/
./hop marketplace install hop-tech-parquet
```

## Who uses admin?

| Action | Identity |
|--------|----------|
| UI setup (A–C) | admin once |
| `publish-wave1-plugins.sh` | admin / deploy user |
| `hop marketplace install` | **anonymous** |

## If the UI still makes no sense

The marketplace client also supports Basic auth with a **read-only** user (not admin):

```bash
export HOP_MARKETPLACE_USERNAME=reader
export HOP_MARKETPLACE_PASSWORD='…'
```

Create that user under **User Management → Users**, grant **Read** on `hop-plugins-local` only, and skip anonymous if preferred.
