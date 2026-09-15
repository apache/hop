#!/usr/bin/env node
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

/*
 * Turn Antora's JSON log into GitHub output: error annotations on the
 * offending .adoc lines, a job summary and, for a pull request, the sticky
 * comment that pr_docs_comment.yml posts on the PR.
 *
 *   node antora-report.mjs <antora.log> <report dir>
 *
 * Reads GITHUB_WORKSPACE and GITHUB_STEP_SUMMARY, plus PR_NUMBER, HEAD_REPO,
 * HEAD_SHA and RUN_URL for the comment. Without PR_NUMBER no comment is written.
 */
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

const [logFile, reportDir] = process.argv.slice(2)
const root = join(process.env.GITHUB_WORKSPACE ?? '', 'hop') + '/'

const rows = []
for (const line of readFileSync(logFile, 'utf8').split('\n')) {
  if (!line.startsWith('{')) continue
  let e
  try {
    e = JSON.parse(line)
  } catch {
    continue
  }
  rows.push({
    level: e.level,
    file: e.file?.path?.replace(root, '') ?? '',
    line: e.file?.line ?? '',
    msg: e.msg,
  })
}
const errors = rows.filter((r) => r.level === 'error' || r.level === 'fatal')
const warnings = rows.filter((r) => r.level === 'warn')

// Annotations: only errors. GitHub shows at most 10 per kind per step and the
// docs still carry ~150 pre-existing warnings.
for (const r of errors) {
  const where = r.file ? `file=${r.file},${r.line ? `line=${r.line},` : ''}` : ''
  console.log(`::error ${where}title=Antora::${r.msg}`)
}

const cell = (s) => String(s).replace(/\|/g, '\\|').replace(/\n/g, ' ')
// docs/hop-user-manual/modules/ROOT/pages/x/y.adoc -> user-manual: x/y.adoc
const short = (file) => file.replace(/^docs\/hop-([a-z]+-manual)\/modules\/ROOT\/pages\//, '$1: ')
const table = (list, name) =>
  list.length
    ? [
        '| File | Line | Message |',
        '|---|---|---|',
        ...list.map((r) => `| ${name(r)} | ${r.line} | ${cell(r.msg)} |`),
      ].join('\n')
    : '_none_'

const summary = [
  '## Antora build',
  `${errors.length} error(s), ${warnings.length} warning(s)`,
  '',
  '### Errors',
  table(errors, (r) => cell(r.file)),
  '',
  '<details><summary>Warnings</summary>',
  '',
  table(warnings, (r) => cell(r.file)),
  '',
  '</details>',
  '',
].join('\n')
if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, summary)

const { PR_NUMBER, HEAD_REPO, HEAD_SHA, RUN_URL } = process.env
if (!PR_NUMBER || !reportDir) process.exit(0)

const linked = (r) =>
  r.file
    ? `[${cell(short(r.file))}](https://github.com/${HEAD_REPO}/blob/${HEAD_SHA}/${r.file}${r.line ? `#L${r.line}` : ''})`
    : ''
// The warnings are mostly pre-existing and not what the PR is about: keep them
// folded and capped so the comment stays readable.
const MAX_WARNINGS = 50
const shown = warnings.slice(0, MAX_WARNINGS)
const body = [
  '<!-- hop-docs-check -->',
  errors.length
    ? `### :x: Documentation build failed with ${errors.length} error(s)`
    : '### :white_check_mark: Documentation build passed',
  '',
  `Antora build of the manuals at ${HEAD_SHA.slice(0, 7)}: ` +
    `${errors.length} error(s), ${warnings.length} warning(s) — [run log](${RUN_URL})`,
  ...(errors.length ? ['', table(errors, linked)] : []),
  ...(warnings.length
    ? [
        '',
        `<details><summary>${warnings.length} warning(s)` +
          `${shown.length < warnings.length ? `, first ${MAX_WARNINGS} shown` : ''}</summary>`,
        '',
        table(shown, linked),
        '',
        '</details>',
      ]
    : []),
  '',
].join('\n')

mkdirSync(reportDir, { recursive: true })
writeFileSync(join(reportDir, 'comment.md'), body)
writeFileSync(join(reportDir, 'pr-number'), `${PR_NUMBER}\n`)
writeFileSync(join(reportDir, 'error-count'), `${errors.length}\n`)
