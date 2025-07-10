local workflows_template = import 'tools/github_workflows/workflows_template.libsonnet';

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
  [
    {
      name: 'Install WinFSP',
      run: 'choco install winfsp',
      'if': "matrix.host.platform_name == 'windows_amd64'",
    },
    {
      name: 'Register WinFSP availability in .bazelrc',
      run: 'echo "common --host_platform=//tools/platforms:host_winfsp" >> .bazelrc',
      'if': "matrix.host.platform_name == 'windows_amd64'",
    },
  ]
)
