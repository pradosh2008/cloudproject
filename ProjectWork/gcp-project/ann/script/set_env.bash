if [ -z $PROFILERUN ]; then
    . $HOME/.bash_profile
fi
case $run_env in
prod)
    ;;
test)
    ;;
dev)
    ;;
*)
    ;;
esac
