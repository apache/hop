# hop-import integration tests

Exercises the `hop-import` command line tool end to end, by shelling out to `hop-import.sh` from
a workflow and asserting on what lands in the target folder. These cover the CLI half of the
front-end parity work in [#8516]; the import dialog half is covered by
`KettleImportDialogLayoutTest` in the `hop-misc-import` plugin.

| Test | What it proves |
| --- | --- |
| `main-0001-import-preserves-run-configurations` | Without `--pipeline-run-configuration` / `--workflow-run-configuration`, the run configuration names in the PDI source survive the import instead of being blanked ([#3814], [#8516]). Also checks that sub-folders are imported by default. |
| `main-0002-import-applies-default-run-configurations` | With both options, every imported pipeline and workflow gets the given run configuration. |
| `main-0003-import-registers-a-project` | `--project` registers the target folder as a Hop project, and a second run resolves that project's home folder without a `-o`. An import that fails on its options registers nothing. |

`subject/pdi` holds the PDI job, transformation and nested job the tests import. Both entries of
`parent.kjb` name a run configuration, which is what tests 0001 and 0002 assert on.

Each test imports into its own folder under `output/`, so they can run in any order. Test 0003
points `HOP_CONFIG_FOLDER` at a throwaway folder as well: registering a project writes
`hop-config.json`, which must not be the one this suite itself runs on.

[#3814]: https://github.com/apache/hop/issues/3814
[#8516]: https://github.com/apache/hop/issues/8516
