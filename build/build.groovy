def commonConfig = [
        gitRepoName: 'rtfw_ndl_eks',
        gitProject: 'RTFW',
        checkmarxEnabled: true,
        devopsConfig: 'src/main/resources/ci/devops.json',
        sonarEnabled: true,
        deployToNexusEnabled: false,
        deployToClusterEnabled: false,
        logicalEnv: 'ds22',
        martYamlChanged: false,
        waitForCompletion: true
]

autoSelectedDataProductPipeline(
        developBranchConfig: commonConfig,
        releaseBranchConfig: commonConfig,
        featureBranchConfig: commonConfig
)

