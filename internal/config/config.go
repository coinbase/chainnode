package config

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/utils/auxiliary"

	"github.com/coinbase/chainnode/config"
	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/utils/retry"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type (
	Config struct {
		ConfigName       string           `mapstructure:"config_name" validate:"required"`
		Chain            ChainConfig      `mapstructure:"chain"`
		Tag              TagConfig        `mapstructure:"tag"`
		AWS              AWSConfig        `mapstructure:"aws"`
		Cadence          CadenceConfig    `mapstructure:"cadence"`
		Workflows        WorkflowsConfig  `mapstructure:"workflows"`
		Controller       ControllerConfig `mapstructure:"controller"`
		Cron             CronConfig       `mapstructure:"cron"`
		TaskPool         TaskPoolConfig   `mapstructure:"task_pool"`
		Storage          StorageConfig    `mapstructure:"storage"`
		SLA              SLAConfig        `mapstructure:"sla"`
		BatchLimitConfig BatchLimitConfig `mapstructure:"batch_limit"`
		Server           ServerConfig     `mapstructure:"server"`

		env Env
	}

	TagConfig struct {
		Stable uint32 `mapstructure:"stable" validate:"required"`
		Latest uint32 `mapstructure:"latest" validate:"required"`
	}

	ChainConfig struct {
		Blockchain           common.Blockchain `mapstructure:"blockchain" validate:"required"`
		Network              common.Network    `mapstructure:"network" validate:"required"`
		Client               ClientConfig      `mapstructure:"client"`
		IrreversibleDistance uint64            `mapstructure:"irreversible_distance" validate:"required"`
	}

	ClientConfig struct {
		ServerName    string        `mapstructure:"server_name"`
		ServerAddress string        `mapstructure:"server_address"`
		ServerHandle  string        `mapstructure:"server_handle"`
		Primary       EndpointGroup `mapstructure:"primary"` // TODO: rename to proxy in secrets
		Validator     EndpointGroup `mapstructure:"validator"`
		Retry         RetryConfig   `mapstructure:"retry"`
	}

	EndpointGroup struct {
		Endpoints         []Endpoint `json:"endpoints"`
		EndpointsFailover []Endpoint `json:"endpoints_failover"`
		UseFailover       bool       `json:"use_failover"`
	}

	// endpointGroup must be in sync with EndpointGroup
	endpointGroup struct {
		Endpoints         []Endpoint `json:"endpoints"`
		EndpointsFailover []Endpoint `json:"endpoints_failover"`
		UseFailover       bool       `json:"use_failover"`
	}

	Endpoint struct {
		Name     string `json:"name"`
		Url      string `json:"url"`
		User     string `json:"user"`
		Password string `json:"password"`
		Weight   uint8  `json:"weight"`
	}

	RetryConfig struct {
		MaxAttempts     int           `mapstructure:"max_attempts"`
		InitialInterval time.Duration `mapstructure:"initial_interval"`
	}

	CadenceConfig struct {
		Address         string `mapstructure:"address" validate:"required"`
		Domain          string `mapstructure:"domain" validate:"required"`
		RetentionPeriod int32  `mapstructure:"retention_period" validate:"required"`
	}

	AWSConfig struct {
		Region       string         `mapstructure:"region" validate:"required"`
		Bucket       string         `mapstructure:"bucket" validate:"required"`
		DynamoDB     DynamoDBConfig `mapstructure:"dynamodb" validate:"required"`
		IsLocalStack bool           `mapstructure:"local_stack"`
		IsResetLocal bool           `mapstructure:"reset_local"`
		AWSAccount   AWSAccount     `mapstructure:"aws_account" validate:"required"`
	}

	CronConfig struct {
		DisableFailover       bool     `mapstructure:"disable_failover"`
		DisableShadow         bool     `mapstructure:"disable_shadow"`
		DisablePollingCanary  bool     `mapstructure:"disable_polling_canary"`
		DisableSLA            bool     `mapstructure:"disable_sla"`
		DisableNodeCanary     bool     `mapstructure:"disable_node_canary"`
		DisableWorkflowStatus bool     `mapstructure:"disable_workflow_status"`
		ExpectedWorkflows     []string `mapstructure:"expected_workflows"`
	}

	DynamoDBConfig struct {
		CollectionTable           string `mapstructure:"collection_table" validate:"required"`
		LogsTable                 string `mapstructure:"logs_table" validate:"required"`
		LogsTablePartitionSize    uint64 `mapstructure:"logs_table_partition_size" validate:"required"`
		LogsByBlockRangeIndexName string `mapstructure:"logs_by_block_range_index_name" validate:"required"`
		MaxDataSize               int    `mapstructure:"max_data_size" validate:"required"`
	}

	StorageConfig struct {
		GlobalCheckpoint    GlobalCheckpointConfig `mapstructure:"global_checkpoint"`
		TraceUploadEnforced bool                   `mapstructure:"trace_upload_enforced"`
	}

	// GlobalCheckpointConfig
	// Global checkpoint refer to checkpoints that contain the global status of the service, including latest checkpoints
	GlobalCheckpointConfig struct {
		// NumberOfShards is the number of shards dedicated for each global checkpoint
		NumberOfShards int `mapstructure:"number_of_shards" validate:"min=1,max=25"`
	}

	WorkflowsConfig struct {
		Workers     []WorkerConfig            `mapstructure:"workers"`
		Ingestor    IngestorWorkflowConfig    `mapstructure:"ingestor"`
		Coordinator CoordinatorWorkflowConfig `mapstructure:"coordinator"`
	}

	WorkerConfig struct {
		TaskList string `mapstructure:"task_list"`
	}

	WorkflowConfig struct {
		WorkflowIdentity             string        `mapstructure:"workflow_identity" validate:"required"`
		TaskList                     string        `mapstructure:"task_list" validate:"required"`
		WorkflowExecutionTimeout     time.Duration `mapstructure:"workflow_execution_timeout" validate:"required"`
		ActivityStartToCloseTimeout  time.Duration `mapstructure:"activity_start_to_close_timeout" validate:"required"`
		ActivityHeartbeatTimeout     time.Duration `mapstructure:"activity_heartbeat_timeout"`
		ActivityRetryMaximumAttempts int32         `mapstructure:"activity_retry_maximum_attempts" validate:"required"`
		Tag                          TagConfig     `mapstructure:"tag"`
	}

	IngestorWorkflowConfig struct {
		WorkflowConfig       `mapstructure:",squash"`
		CheckpointSize       uint64        `mapstructure:"checkpoint_size" validate:"required"`
		CheckpointSizeAtTip  uint64        `mapstructure:"checkpoint_size_at_tip" validate:"required"`
		BatchSize            uint64        `mapstructure:"batch_size" validate:"required"`
		Parallelism          uint64        `mapstructure:"parallelism" validate:"required"`
		MiniBatchSize        uint64        `mapstructure:"mini_batch_size" validate:"required"`
		MiniBatchParallelism uint64        `mapstructure:"mini_batch_parallelism"`
		Backoff              time.Duration `mapstructure:"backoff" validate:"required"`
	}

	CoordinatorWorkflowConfig struct {
		WorkflowConfig       `mapstructure:",squash"`
		CheckpointInterval   time.Duration    `mapstructure:"checkpoint_interval" validate:"required"`
		SynchronizerInterval time.Duration    `mapstructure:"synchronizer_interval" validate:"required"`
		Ingestors            []IngestorConfig `mapstructure:"ingestors" validate:"required"`
		InterruptTimeout     time.Duration    `mapstructure:"interrupt_timeout" validate:"required"`
		EventTag             uint32           `mapstructure:"event_tag"`
	}

	IngestorConfig struct {
		Collection           string        `mapstructure:"collection" validate:"required"`
		Synchronized         bool          `mapstructure:"synchronized"`
		CheckpointSize       uint64        `mapstructure:"checkpoint_size"`
		BatchSize            uint64        `mapstructure:"batch_size"`
		Parallelism          uint64        `mapstructure:"parallelism"`
		MiniBatchSize        uint64        `mapstructure:"mini_batch_size"`
		MiniBatchParallelism uint64        `mapstructure:"mini_batch_parallelism"`
		Backoff              time.Duration `mapstructure:"backoff"`
	}

	ControllerConfig struct {
		Handler      HandlerConfig        `mapstructure:"handler"`
		ReverseProxy []ReverseProxyConfig `mapstructure:"reverse_proxy" validate:"dive"`
	}

	HandlerConfig struct {
		ShadowPercentage   int            `mapstructure:"shadow_percentage" validate:"min=0,max=100"`
		MaxBlockNumberDiff int            `mapstructure:"max_block_number_diff" validate:"required"`
		MethodConfigs      []MethodConfig `mapstructure:"methods"`
	}

	MethodConfig struct {
		MethodName       string `mapstructure:"method_name" validate:"required"`
		ShadowPercentage int    `mapstructure:"shadow_percentage" validate:"min=0,max=100"`
	}

	// ReverseProxyConfig defines a path to be handled by the "proxy" endpoint provider.
	// For example, assuming "proxy" endpoint provider is configured as "https://proxy.net:9005",
	// by setting path to "/v1/graphql" and target to "/graphql",
	// all requests to https://localhost:8000/v1/graphql will be handled by
	// https://proxy.net:9005/graphql.
	ReverseProxyConfig struct {
		Path   string `mapstructure:"path" validate:"required,startswith=/"`
		Target string `mapstructure:"target" validate:"required,startswith=/"`
	}

	TaskPoolConfig struct {
		Size int `mapstructure:"size" validate:"required"`
	}

	SLAConfig struct {
		Tier                      int           `mapstructure:"tier" validate:"required"` // 1 for high urgency; 2 for low urgency; 3 for work in progress.
		BlockHeightDelta          uint64        `mapstructure:"block_height_delta" validate:"required"`
		BlockTimeDelta            time.Duration `mapstructure:"block_time_delta" validate:"required"`
		TimeSinceLastBlock        time.Duration `mapstructure:"time_since_last_block" validate:"required"`
		ValidatorBlockHeightDelta uint64        `mapstructure:"validator_block_height_delta" validate:"required"`
	}

	ConfigOption func(options *configOptions)

	Env string

	AWSAccount string

	BaseWorkflowConfig interface {
		Base() *WorkflowConfig
	}

	configOptions struct {
		Blockchain common.Blockchain `validate:"required"`
		Network    common.Network    `validate:"required"`
		Env        Env               `validate:"required,oneof=production development local"`
	}

	BatchLimitConfig struct {
		DefaultLimit int `mapstructure:"default_limit" validate:"required"`
	}

	ServerConfig struct {
		BindAddress string `mapstructure:"bind_address" validate:"required"`
	}

	// derivedConfig defines a callback where a config struct can override its fields based on the global config.
	// For example, WorkflowConfig implements this interface to copy the global tag into its own struct.
	derivedConfig interface {
		DeriveConfig(cfg *Config)
	}
)

const (
	EnvVarConfigName  = "CHAINNODE_CONFIG_NAME"
	EnvVarEnvironment = "CHAINNODE_ENVIRONMENT"
	EnvVarProjectName = "CHAINNODE_PROJECT_NAME"
	EnvVarTestType    = "TEST_TYPE"
	EnvVarCI          = "CI"

	Namespace         = "chainnode"
	DefaultConfigName = "ethereum-mainnet"

	EnvBase        Env = "base"
	EnvLocal       Env = "local"
	EnvProduction  Env = "production"
	EnvDevelopment Env = "development"
	envSecrets     Env = "secrets" // .secrets.yml is merged into local.yml

	AWSAccountDataSharedDev AWSAccount = "development"
	AWSAccountProduction    AWSAccount = "production"

	ServerHandle = "/v1"

	placeholderPassword = "<placeholder>"

	cadenceAddressPlaceholder   = "cadence-address-placeholder"
	cadenceAddressLocal         = "localhost:7233"
	cadenceDomainFormat         = "chainnode-%v"
	collectionTableFormat       = "chainnode-collection-%s"
	collectionS3Format          = "chainnode-collections-%s-%s"
	logsTableFormat             = "chainnode-logs-%s"
	logsTablePartitionSize      = 500 //DO NOT CHANGE the value
	logsByBlockRangeIndexFormat = "chainnode-logs-by-block-range-%s"

	// ddb max item size limit is 400KB. 4KB reserved for attribute names
	ddbMaxDataSize = 396 * 1024

	tagBlockchain = "blockchain"
	tagNetwork    = "network"
	tagTier       = "tier"

	workflowCoordinator          = "coordinator"
	workflowBlocksIngestorFormat = "ingestor/blocks/%d"

	currentFileName = "/internal/config/config.go"
)

var (
	_ derivedConfig = (*ClientConfig)(nil)
	_ derivedConfig = (*CadenceConfig)(nil)
	_ derivedConfig = (*WorkflowConfig)(nil)
	_ derivedConfig = (*AWSConfig)(nil)
	_ derivedConfig = (*HandlerConfig)(nil)
	_ derivedConfig = (*CronConfig)(nil)

	AWSAccountEnvMap = map[AWSAccount]Env{
		"":                      EnvLocal,
		AWSAccountDataSharedDev: EnvDevelopment,
		AWSAccountProduction:    EnvProduction,
	}

	AWSAccountShortMap = map[AWSAccount]string{
		AWSAccountDataSharedDev: "dev",
		AWSAccountProduction:    "prod",
	}
)

func New(opts ...ConfigOption) (*Config, error) {
	validate := validator.New()

	// Get configname, such as "ethereum-mainnet"
	configName := getConfigName()

	// Get "blockchain", "network", "env"
	configOpts, err := getConfigOptions(configName, opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to get config options %w", err)
	}

	if err := validate.Struct(configOpts); err != nil {
		return nil, xerrors.Errorf("failed to validate config options: %w", err)
	}

	configReader, err := getConfigData(Namespace, EnvBase, configOpts.Blockchain, configOpts.Network)
	if err != nil {
		return nil, xerrors.Errorf("failed to locate config file: %w", err)
	}

	cfg := Config{
		env: configOpts.Env,
	}

	v := viper.New()
	// First, read the data in base.yml
	v.SetConfigName(string(EnvBase))
	v.SetConfigType("yaml")
	v.AutomaticEnv()
	v.AllowEmptyEnv(true)

	v.SetEnvPrefix("CHAINNODE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Set default values.
	// Note that the default values may be overridden by environment variable or config file.
	if cfg.Env() == EnvLocal {
		v.SetDefault("aws.local_stack", true)
	}
	if cfg.IsTest() {
		v.SetDefault("aws.local_stack", true)
		v.SetDefault("aws.reset_local", true)
	}

	// Read the data in base.yml
	if err := v.ReadConfig(configReader); err != nil {
		return nil, xerrors.Errorf("failed to read config: %w", err)
	}

	// Merge in the env-specific config, such as development.yml
	if err := mergeInConfig(v, configOpts, configOpts.Env); err != nil {
		return nil, xerrors.Errorf("failed to merge in %v config: %w", configOpts.Env, err)
	}

	// Merge in .secrets.yml if available.
	if err := mergeInConfig(v, configOpts, envSecrets); err != nil {
		return nil, xerrors.Errorf("failed to merge in %v config: %w", envSecrets, err)
	}

	if err := v.Unmarshal(&cfg, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		mapstructure.TextUnmarshallerHookFunc(),
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		stringToBlockchainHookFunc(),
		stringToNetworkHookFunc(),
	))); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal config: %w", err)
	}

	cfg.setDerivedConfigs(reflect.ValueOf(&cfg))

	if err := validate.Struct(&cfg); err != nil {
		return nil, xerrors.Errorf("failed to validate config: %w", err)
	}

	return &cfg, nil
}

func getConfigName() string {
	configName, ok := os.LookupEnv(EnvVarConfigName)
	if !ok {
		configName = DefaultConfigName
	}
	return configName
}

func mergeInConfig(v *viper.Viper, configOpts *configOptions, env Env) error {
	// Merge in the env-specific config if available.
	if configReader, err := getConfigData(Namespace, env, configOpts.Blockchain, configOpts.Network); err == nil {
		v.SetConfigName(string(env))
		if err := v.MergeConfig(configReader); err != nil {
			return xerrors.Errorf("failed to merge config %v: %w", configOpts.Env, err)
		}
	}
	return nil
}

func (c *Config) Env() Env {
	return c.env
}

// ShortEnv returns an abbreviation of Env.
// It is equivalent to `{{constant::short_name}}` defined in aws-resources.
func (c *Config) ShortEnv() string {
	shortEnv := "dev"
	if val, exists := AWSAccountShortMap[c.AWS.AWSAccount]; exists {
		shortEnv = val
	}
	return shortEnv
}

func (c *Config) Blockchain() common.Blockchain {
	return c.Chain.Blockchain
}

func (c *Config) Network() common.Network {
	return c.Chain.Network
}

func (c *Config) Tier() int {
	return c.SLA.Tier
}

func (c *Config) GetCommonTags() map[string]string {
	return map[string]string{
		tagBlockchain: c.Blockchain().GetName(),
		tagNetwork:    c.Network().GetName(),
		tagTier:       strconv.Itoa(c.Tier()),
	}
}

func (c *Config) IsCI() bool {
	return os.Getenv(EnvVarCI) != ""
}

func (c *Config) IsUnitTest() bool {
	return os.Getenv(EnvVarTestType) == "unit"
}

func (c *Config) IsIntegrationTest() bool {
	return os.Getenv(EnvVarTestType) == "integration"
}

func (c *Config) IsFunctionalTest() bool {
	return os.Getenv(EnvVarTestType) == "functional"
}

func (c *Config) IsTest() bool {
	return os.Getenv(EnvVarTestType) != ""
}

func (c *Config) IsCollectionSynchronized(collection api.Collection) bool {
	for _, ingestor := range c.Workflows.Coordinator.Ingestors {
		if api.Collection(ingestor.Collection) == collection {
			return ingestor.Synchronized
		}
	}
	return false
}

// setDerivedConfigs recursively calls DeriveConfig on all the derivedConfig.
func (c *Config) setDerivedConfigs(v reflect.Value) {
	if v.CanInterface() {
		if oc, ok := v.Interface().(derivedConfig); ok {
			oc.DeriveConfig(c)
			return
		}
	}

	elem := v.Elem()
	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		if field.Kind() == reflect.Struct {
			c.setDerivedConfigs(field.Addr())
		}
	}
}

func getConfigOptions(configName string, opts ...ConfigOption) (*configOptions, error) {
	blockchain, network, err := ParseConfigName(configName)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse config name: %w", err)
	}

	awsAccount := AWSAccount(os.Getenv(EnvVarEnvironment))
	env := AWSAccountEnvMap[awsAccount]

	configOpts := &configOptions{
		Blockchain: blockchain,
		Network:    network,
		Env:        env,
	}

	for _, opt := range opts {
		opt(configOpts)
	}
	return configOpts, nil
}

func ParseConfigName(configName string) (common.Blockchain, common.Network, error) {
	// Normalize the config name by replacing "-" with "_".
	configName = strings.ReplaceAll(configName, "-", "_")

	splitString := strings.Split(configName, "_")
	if len(splitString) != 2 {
		return common.Blockchain_BLOCKCHAIN_UNKNOWN, common.Network_NETWORK_UNKNOWN, xerrors.Errorf("config name is invalid: %v", configName)
	}

	blockchainName := splitString[0]
	blockchain, err := auxiliary.ParseBlockchain(blockchainName)
	if err != nil {
		return common.Blockchain_BLOCKCHAIN_UNKNOWN, common.Network_NETWORK_UNKNOWN, xerrors.Errorf("failed to parse blockchain from config name %v: %w", configName, err)
	}

	networkName := fmt.Sprintf("%v_%v", splitString[0], splitString[1])
	network, err := auxiliary.ParseNetwork(networkName)
	if err != nil {
		return common.Blockchain_BLOCKCHAIN_UNKNOWN, common.Network_NETWORK_UNKNOWN, xerrors.Errorf("failed to parse network from config name %v: %w", configName, err)
	}

	return blockchain, network, nil
}

func stringToBlockchainHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t != reflect.TypeOf(common.Blockchain_BLOCKCHAIN_UNKNOWN) {
			return data, nil
		}

		return common.Blockchain_value[data.(string)], nil
	}
}

func stringToNetworkHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t != reflect.TypeOf(common.Network_NETWORK_UNKNOWN) {
			return data, nil
		}

		return common.Network_value[data.(string)], nil
	}
}

func getConfigData(namespace string, env Env, blockchain common.Blockchain, network common.Network) (io.Reader, error) {
	blockchainName := blockchain.GetName()
	networkName := strings.TrimPrefix(network.GetName(), blockchainName+"-")

	if env == envSecrets {
		// .secrets.yml is intentionally not embedded in config.Store.
		// Read it from the file system instead.
		_, filename, _, ok := runtime.Caller(0)
		if !ok {
			return nil, xerrors.Errorf("failed to recover the filename information")
		}
		rootDir := strings.TrimSuffix(filename, currentFileName)
		configPath := fmt.Sprintf("%v/config/%v/%v/%v/.secrets.yml", rootDir, namespace, blockchainName, networkName)
		reader, err := os.Open(configPath) // #nosec G304 - potential file inclusion via variable
		if err != nil {
			return nil, xerrors.Errorf("failed to read config file %v: %w", configPath, err)
		}
		return reader, nil
	}

	configPath := fmt.Sprintf("%s/%v/%v/%v.yml", namespace, blockchainName, networkName, env)

	return config.Store.Open(configPath)
}

func WithBlockchain(blockchain common.Blockchain) ConfigOption {
	return func(opts *configOptions) {
		opts.Blockchain = blockchain
	}
}

func WithNetwork(network common.Network) ConfigOption {
	return func(opts *configOptions) {
		opts.Network = network
	}
}

func WithEnvironment(env Env) ConfigOption {
	return func(opts *configOptions) {
		opts.Env = env
	}
}

func (c *ClientConfig) DeriveConfig(cfg *Config) {
	if cfg.Env() == EnvLocal {
		c.ServerAddress = "http://localhost:8000"
	}
	c.ServerName = fmt.Sprintf("%s-%s-%s-%s", Namespace, cfg.Chain.Blockchain.GetName(), cfg.Chain.Network.GetName(), cfg.Env())
	c.ServerHandle = ServerHandle
}

func (c *CadenceConfig) DeriveConfig(cfg *Config) {
	if c.Address == "" {
		switch cfg.Env() {
		case EnvLocal:
			c.Address = cadenceAddressLocal
		default:
			// TODO: Add a default temporal address to run the service out of local environment.
			c.Address = cadenceAddressPlaceholder
		}
	}

	if c.Domain == "" {
		c.Domain = fmt.Sprintf(cadenceDomainFormat, cfg.ConfigName)
	}
}

func (c *CronConfig) DeriveConfig(cfg *Config) {
	if len(c.ExpectedWorkflows) == 0 {
		expectedWorkflows := make([]string, 2)
		expectedWorkflows[0] = workflowCoordinator
		expectedWorkflows[1] = fmt.Sprintf(workflowBlocksIngestorFormat, cfg.Tag.Stable)

		c.ExpectedWorkflows = expectedWorkflows
	}
}

func (c *WorkflowConfig) Base() *WorkflowConfig {
	return c
}

func (c *WorkflowConfig) GetEffectiveTag(tag uint32) uint32 {
	return c.Tag.GetEffectiveTag(tag)
}

func (c *WorkflowConfig) DeriveConfig(cfg *Config) {
	if c.Tag == (TagConfig{}) {
		// Derive from the global tag.
		c.Tag = cfg.Tag
	}
}

func (c *TagConfig) GetEffectiveTag(tag uint32) uint32 {
	if tag == 0 {
		return c.Stable
	}

	return tag
}

func (c *AWSConfig) DeriveConfig(cfg *Config) {
	if c.Bucket == "" {
		c.Bucket = fmt.Sprintf(collectionS3Format, cfg.ConfigName, cfg.ShortEnv())
	}

	if c.DynamoDB.CollectionTable == "" {
		c.DynamoDB.CollectionTable = fmt.Sprintf(collectionTableFormat, cfg.ConfigName)
	}

	if c.DynamoDB.LogsTable == "" {
		c.DynamoDB.LogsTable = fmt.Sprintf(logsTableFormat, cfg.ConfigName)
	}

	if c.DynamoDB.LogsTablePartitionSize == 0 {
		// DO NOT CHANGE the value of LogsTablePartitionSize unless overriding the value in tests only
		c.DynamoDB.LogsTablePartitionSize = logsTablePartitionSize
	}

	if c.DynamoDB.LogsByBlockRangeIndexName == "" {
		c.DynamoDB.LogsByBlockRangeIndexName = fmt.Sprintf(logsByBlockRangeIndexFormat, cfg.ConfigName)
	}

	if c.DynamoDB.MaxDataSize == 0 {
		c.DynamoDB.MaxDataSize = ddbMaxDataSize
	}
}

func (c *ClientConfig) Empty() bool {
	for _, cfg := range []*EndpointGroup{&c.Primary, &c.Validator} {
		if len(cfg.Endpoints) == 0 {
			return true
		}

		for _, endpoint := range cfg.Endpoints {
			if endpoint.Password == placeholderPassword {
				return true
			}
		}
	}

	return false
}

func (e *EndpointGroup) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		return nil
	}

	var eg endpointGroup
	err := json.Unmarshal(text, &eg)
	if err != nil {
		return xerrors.Errorf("failed to parse EndpointGroup JSON: %w", err)
	}

	if len(eg.Endpoints) == 0 {
		return xerrors.New("endpoints is empty")
	}

	if eg.UseFailover && len(eg.EndpointsFailover) == 0 {
		return xerrors.New("endpoints_failover is empty")
	}

	e.Endpoints = eg.Endpoints
	e.EndpointsFailover = eg.EndpointsFailover
	e.UseFailover = eg.UseFailover

	for _, endpoints := range [][]Endpoint{e.Endpoints, e.EndpointsFailover} {
		for _, endpoint := range endpoints {
			if endpoint.Name == "" {
				return xerrors.New("empty endpoint.Name")
			}
			if endpoint.Url == "" {
				return xerrors.New("empty endpoint.URL")
			}
		}
	}
	return nil
}

func (c *HandlerConfig) DeriveConfig(cfg *Config) {
	if c.MaxBlockNumberDiff == 0 {
		c.MaxBlockNumberDiff = int(cfg.Chain.IrreversibleDistance)
	}
}

func (c *RetryConfig) NewRetry(opts ...retry.Option) retry.Retry {
	if c.MaxAttempts > 0 {
		opts = append(opts, retry.WithMaxAttempts(c.MaxAttempts))
	}

	if c.InitialInterval > 0 {
		opts = append(opts, retry.WithBackoffFactory(func() retry.Backoff {
			backoff := retry.DefaultBackoffFactory()
			backoff.InitialInterval = c.InitialInterval
			return backoff
		}))
	}

	return retry.New(opts...)
}
