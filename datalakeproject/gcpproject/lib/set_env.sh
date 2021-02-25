if [ -z $PROFILERUN ]; then
    . $HOME/.bash_profile
fi
# NOTE: env_data_root CANNOT have leading '/' !
case $run_env in
prod)
    export default_bucket="p-ascena-aadp-landing-01"
    export env_data_root="data"
    export schema_path="${HOME}/schema"
    export script_path="${HOME}/script"
    export project="p-ascn-da-aadp-001"
    ;;
test)
    export default_bucket="not-implemented"
    export env_data_root="data/test"
    export schema_path="${HOME}/schema"
    export script_path="${HOME}/script"
    export project="not-implemented"
    ;;
dev)
    export default_bucket="p-asna-datasink-003"
    export env_data_root="data/dev"
    export schema_path="${HOME}/schema"
    export script_path="${HOME}/script"
    export project="p-asna-analytics-002"
    ;;
*)
    ;;
esac
