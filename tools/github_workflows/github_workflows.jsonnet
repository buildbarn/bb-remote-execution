local workflows_template = import 'external/com_github_buildbarn_bb_storage/tools/github_workflows/workflows_template.libsonnet';

workflows_template.getWorkflows(
  [
    'bb_noop_worker',
    'bb_runner',
    'bb_scheduler',
    'bb_virtual_tmp',
    'bb_worker',
    'fake_python',
    'fake_xcrun',
  ],
  [
    'bb_noop_worker:bb_noop_worker',
    'bb_runner:bb_runner_bare',
    'bb_runner:bb_runner_installer',
    'bb_scheduler:bb_scheduler',
    'bb_worker:bb_worker',
  ],
)
