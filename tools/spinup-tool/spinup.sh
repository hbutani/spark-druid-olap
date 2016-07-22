#!/bin/bash

#Global Vars:

# First, colours:
BOLD='\033[0;1m' #(OR USE 31)
CYAN='\033[0;36m'
PURPLE='\033[0;35m'
GREEN='\033[0;32m'
BROWN='\033[0;33m'
RED='\033[1;31m'
NC='\033[0m' # No Color

# Framework variables:
interactive="true"
output="true"
changed=""
tempLoc="/tmp/"

# Specific Internal Vars:
CLUSTER_ID=""
MASTER_INSTANCE_ID=""
SLAVE_INSTANCE_IDS=""

MASTER_PRIVATE_HOSTNAME=""
MASTER_PRIVATE_IP=""
MASTER_PUBLIC_HOSTNAME=""
MASTER_PUBLIC_IP=""

# Specific Function Variables:
replacements=""
provisionScriptLoc=""

# Options Array:
options=(
    "EMR_Provision"
    "EMR_Spinup"
    "Obtain_IDs_And_IPs"
    "Configure_File_Processor"
    "psb_Start"
    "psb_S3Tarball"
    "psb_Config_Files"
    "psb_Misc"
    "EMR_Wait"
    "psb_Run"
    "Start_Master"
    "Start_Slaves"
    "Add2Hosts"
)

#Now the functions..
function print
{
    if [[ "$output" != "true" ]]; then return; fi

	N=0
	n="-e"

	if [[ "$*" == *" -n"* ]]; then
		N=1
		n="-ne"
	fi

	if [ "$#" -eq $((1 + $N)) ]; then
		echo $n $1
	elif [ "$#" -eq $((2 + $N)) ]; then
		printf ${2} && echo $n $1 && printf ${NC}
	else
		#printf ${RED} && echo "Error in print. Received: $*" && printf ${NC}
        printf ${RED} && echo "Received: $*" && printf ${NC}
	fi
}

function checkDependencies
{
	pathToExecutable=$(which aws)

	if [ -x "$pathToExecutable" ]; then
		print "AWS CLI Tools found, continuing.."
	else
		print "AWS CLI Tools missing. More info here: http://amzn.to/UWz3ON" $RED
	fi

    #NOTE!!
    #Other dependencies are:
    #  That the aws cli tools are configured for text output (as opposed to table output)
    #+ which is the default
    #  And that a bucket named tmp exists in S3.
    #       Nvm, resolved. Also that's not possible, so...
}

function checkSetConfigFile
{
	# Check if file location was provided: (May be redundant)..
	if [ -z $1 ]; then
		print "No config file supplied. Run again with -c <location of config file> or without the -c flag." $RED
		exit 1
	fi

	# Now check if the file actually exists:
	if [ -e $1 ]; then
		echo "Loading config file.."

		#  Possibly use some form of verification here (ex: abort if things other than variable declarations exist 
		#+ in the file..)
		#  But, for now, assuming the config file is trustworthy.

		source $1
	else
		print "Specified config file does not exist. Try again or run without arguments to use interactive mode." $RED
		exit 1
	fi
}

function processArguments
{
    if [[ $UID != 0 ]]; then
        echo "Please run this script with sudo:"
        echo "sudo $0 $*"
        exit 1
    fi

    if [ -z "$1" ]; then
		#print "No mode set, defaulting to Interactive Mode." $PURPLE
        #Changed because this seems to be more in line with what people expect..
        #So, no arguments is not an option.
        helpText 1
	fi

	while getopts "hiqQDc:" flag; do
		case $flag in
			h)  #Prints help text
				helpText 0
				;;
			i)	#Interactive Mode; no longer redundant
				print "Interactive Mode set." $PURPLE
				;;
            q)  #Quiet Mode (no interactive fallback)
                interactive="false"
                ;;
            Q)  #QuietQuiet Mode (quiet + no output)
                interactive='false'
                output="nope"
                ;;
            D)  #Debug Mode
                set -x
                ;;
            c)  #Config File Mode
                CONFLOC=($OPTARG) #Because getopts is broken.
                checkSetConfigFile "${CONFLOC[0]}"
                ;;
            \?) #Invalid
                helpText 1
                ;;
		esac
	done
}

function ec2Run
{
    local OPTIND

    while getopts ":h" flag; do
        case $flag in
            h)  #Prints help text -- not really needed in this case, but in case of spin-off
                print "usage: ec2Run <m/s/ms/sm> <s3 script location> <user to run as> <working dir> <comment>"
                print "\t If given the above parameters, runs the given s3 script on the nodes specified."
                print "\t Otherwise returns an error."
                return 0
                ;;
        esac
    done

    # Verification Steps:
    if [[ "$#" != 5 ]]; then print "Incorrect number of params on ec2Run call..." $RED; return 1; fi

    if [[ "$1" != *"m"* ]] && [[ "$1" != *"s"* ]]; then return 2; fi

    s3Exists $2
    if [[ "$?" == 0 ]]; then print "Specified s3 script for ec2Run does not exist..." $RED; return 3; fi

    # Parameter Processing:
    local params="" instance_ids=""

    if [[ "$1" == *"m"* ]]; then instance_ids+=(${MASTER_INSTANCE_ID[@]}); fi
    if [[ "$1" == *"s"* ]]; then instance_ids+=(${SLAVE_INSTANCE_IDS[@]}); fi

    runPath="/tmp/run-$(date +%s%N).sh"

    cat << EOF > ${tempLoc}params.json
{
    "commands":["aws s3 cp $2 $runPath; chmod +x $runPath; sudo -u $3 bash $runPath"],
    "workingDirectory":["$4"],
    "executionTimeout":["3600"]
}
EOF

    params+="--document-name AWS-RunShellScript --timeout-seconds 600 "
    params+="--instance-ids ${instance_ids[@]} "
    params+="--parameters file://${tempLoc}params.json "
    params+="--comment $5"

    # Run!
    print "Running script on $(( ${#instance_ids[@]} - 1 )) EC2 instance(s)..." $PURPLE
    
    aws ssm send-command $params

    return $?
}

function s3Exists
{
    local OPTIND

    while getopts ":h" flag; do
        case $flag in
            h)  #Prints help text -- not really needed in this case, but in case of spin-off
                print "usage: s3Exists <s3 file path OR start of path>"
                print "\t If given a valid s3 path (ex: s3://<bucket-name>/<file-name>) "
                print "\t returns the number of files that match."
                print "\t If no files match, returns 0."
                return 0
                ;;
        esac
    done

    #Hacks and workarounds, courtesy of SO: stackoverflow.com/q/17455782
    count=$(aws s3 ls $1 | wc -l)

    return $count
}

function copyFile
{
    local OPTIND

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text -- not really needed in this case..
                print "usage: copyFile <inital file location> <copy location>"
                print "\t Where both file locations are local, s3, or github raw file paths"
                print "\t Copy location should be a file path, not a directory path"
                return 0
                ;;
            v)  #Verification Mode
                if [[ "$#" -gt 3 ]] || [[ "$#" -lt 2 ]]; then
                    print "Error: call with <inital file location> <copy location>"
                    return 1
                fi

                #Could make the http/https thing one regex, but if it ain't broke..
                if [[ "$2" == "s3://"* ]]; then
                    s3Exists $2
                    if [[ "$?" == 0 ]]; then print "Specified S3 file does not exist." $RED; return 1; fi

                elif [[ "$2" == "http://"* ]] || [[ "$2" == "https://"* ]]; then :

                elif [ -e $2 ]; then :

                else print "Invalid file path \"$2\"" $RED; return 1
                fi

                # if [[ "$3" == "s3://"* ]] || [ -e $3 ]; then :
                # else print "Invalid file path \"$3\"" $RED; return 1
                # fi

                return 0
                ;;
        esac
    done

    #Shouldn't be needed, but still..
    if [[ "$#" -lt 2 ]]; then
        print "Error: call with <inital file location> <copy location>"
        return 1
    fi

    #Copy file to intermediary..
    if [[ "$1" == "s3://"* ]]; then
        aws s3 cp $1 ${tempLoc}intermediary.file
    elif [[ "$1" == "http://"* ]] || [[ "$1" == "https://"* ]]; then
        curl $1 -s -o ${tempLoc}intermediary.file
    elif [ -e $1 ]; then
        cp $1 ${tempLoc}intermediary.file
    else
        print "Error: Software developer is a literal garbage fire..." $RED
        exit 1
    fi

    #Copy intermediary to file destination..
    if [[ "$2" == "s3://"* ]]; then
        aws s3 cp ${tempLoc}intermediary.file $2
    else
        cp ${tempLoc}intermediary.file $2
    fi

    rm ${tempLoc}intermediary.file

    return 0
}

function fileProcessor
{
    if [[ "$#" -lt 2 ]]; then
        print "Error: call with <fileLocation> \${<key:value pairs array>[@]}" $RED
        return 1
    fi

    #NOTE: Currently has basically no safeties or error checking mechanisms
    #Doesn't check if:
    # - Original file exists | (!!NVM, taken care of!!)
    # - Output location exists and is writable | (!!NVM, we are now root, ALL IS WRITABLE!!)
    # - File with same name already exists in output location (oops...it's probably /tmp/ though, so.. too bad)
    # - key-value pairs are sane (formatted correctly and not duplicated, etc.) (A job for the configure method)

    if [ ! -e $1 ]; then return 2; fi

    nameOfFile=$(basename $1)
    
    local args=""
    if [[ $(uname -s) == "Darwin" ]]; then args='.bak'; print "Mac OS X host detected, adjusting sed command..."; fi

    for pair in "$@"; do
        if [[ "$pair" == *":"* ]]; then
            kii="${pair%%:*}" #calling the var key breaks other things that use key 
            val="${pair#*:}"  #Same with value (use local next time..)

            val="$(internalVarReplacer $val)"

            print "\tReplacing \"$kii\" with \"$val\" in $nameOfFile.."
            sed -i '$args' "s/$kii/$val/g" $1
        fi
    done

    print "$nameOfFile processed." $BROWN
    return 0
}

function jsonParser
{
    return $(echo $1 | python -c "import json,sys;print json.load(sys.stdin)$2;")
}

function internalVarReplacer
{
    case $1 in
        "MASTER_PRIVATE_HOSTNAME")  #>> $MASTER_PRIVATE_HOSTNAME
                                    echo $MASTER_PRIVATE_HOSTNAME
                                    ;;
        "MASTER_PRIVATE_IP")        #>> $MASTER_PRIVATE_IP
                                    echo $MASTER_PRIVATE_IP
                                    ;;
        "MASTER_PUBLIC_HOSTNAME")   #>> $MASTER_PUBLIC_HOSTNAME
                                    echo $MASTER_PUBLIC_HOSTNAME
                                    ;;
        "MASTER_PUBLIC_IP")         #>> $MASTER_PUBLIC_IP
                                    echo $MASTER_PUBLIC_IP
                                    ;;
        *)                          #>> $1 (no change)
                                    echo $1
                                    ;;
    esac
    
    return 0
}

function fun_Template
{
    local OPTIND
    verifyMode='false'

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: Template [-abs] [-p <text>] [-t <text>]"
                print ""
                print "\t -a                  Prints out apple."
                print "\t -b                  Prints out banana."
                print "\t -s                  Prints out something else.."
                print "\t -p <text>           Prints out purple text."
                print "\t -t <text>           Prints out bold text."
                return 0
                ;;
            v)  #Verification Mode
                print "Verify mode" $BOLD
                verifyMode="true"
                ;;
        esac
    done

    unset OPTIND
    if [[ "$verifyMode" == "true" ]]; then
        while getopts "vabsp:t:" flag; do
            case $flag in
                a)  ;;
                b)  ;;
                s)  ;;
                p)  ;;
                t)  ;;
                \?) print "$flag is invalid." $RED; return 1;;
            esac
        done
        return 0
    fi

    while getopts "absp:t:" flag; do
        case $flag in
            a)  #Apple
                print "Apple! "
                ;;
            b)  #Banana
                print "banananananananana. Banana"
                ;;
            s)  #So(ng)mething else..
                print "I like to ote, ote, ote oh-ples and bo-no-nos"
                ;;
            p)  #Purple
                print $OPTARG $PURPLE
                ;;
            t)  #true-bold
                print $OPTARG $BOLD
                ;;
            \?) #Broken
                print "You've done something very very wrong." $RED
                ;;
        esac
    done
}

function fun_Sample
{
    print "fun_Sample was called with $*" $CYAN

    return 0
}

function fun_EMR_Instance_ID
{
    local OPTIND
    verifyMode='false'

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: EMR_Instance_ID <instance-id>"
                print ""
                print "\t If given an EMR Instance ID, sets the corresponding internal var to the given ID."
                return 0
                ;;
            v)  #Verification Mode
                if [[ "$2" != "j-"* ]]; then print "Given ID is invalid.." $RED; return 1; fi
                if [[ $(aws emr list-instances --cluster-id ${2} --query Instances[0].Status.State) != *"RUNNING"* ]]; then
                    print "Cluster exists but is not running.." $RED; return 1; fi
                return 0
                ;;
        esac
    done

    CLUSTER_ID=${1}
    print "Cluster ID successfully set to $CLUSTER_ID" $PURPLE

    return 0
}

function fun_EMR_Provision
{
    local OPTIND
    verifyMode='false'

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: EMR_Provision [-a <availability-area>] [-s <s3 bucket name>]"
                print ""
                print "\t -a                  Required; specify availability area of the cluster. (ex: us-east-1)"
                print "\t -s                  Required; specify bucket name for temp storage of setup materials."
                return 0
                ;;
            v)  #Verification Mode
                verifyMode="true"
                ;;
        esac
    done

    unset OPTIND
    if [[ "$verifyMode" == "true" ]]; then
        COUNT=0
        while getopts "va:s:" flag; do
            case $flag in
                a)  COUNT=$((COUNT+1))
                    ;;
                s)  COUNT=$((COUNT+3))
                    ;;
                \?) print "$flag is invalid." $RED; return 1;;
            esac
        done
        if [ "$COUNT" -eq 4 ]; then 
            return 0
        else
            return 1
        fi
    fi

    S3PTH=""
    AZONE=""
    while getopts "va:s:" flag; do
        case $flag in
            a)  AZONE="$OPTARG"
                ;;
            s)  S3PTH="$OPTARG"
                ;;
            \?) #Broken
                print "You've done something very very wrong." $RED
                ;;
        esac
    done

    aws s3 mb $S3PTH

    cat << EOF > "${tempLoc}provision.sh"
mkdir -p /tmp/ssm
cd /tmp/ssm
curl -O https://amazon-ssm-${AZONE}.s3.amazonaws.com/latest/linux_amd64/amazon-ssm-agent.rpm -o /tmp/ssm/amazon-ssm-agent.rpm
sudo yum install -y amazon-ssm-agent.rpm
sudo status amazon-ssm-agent

touch /tmp/provisioner-was-here
EOF

    aws s3 cp "${tempLoc}provision.sh" $S3PTH
    print "Provision Script (for SSM-Agent) copied to $S3PTH" $PURPLE
    return 0
}

function fun_EMR_Spinup
{
    local OPTIND

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                sleep 1
                aws emr create-cluster help
                return 0
                ;;
            v)  #Verification Mode
                #No verification for now...
                return 0
                ;;
        esac
    done

    temp=$(aws emr create-cluster ${EMR_Spinup[*]})

    #Parse $temp (command output) to extract cluster-id in case of table mode
    arr=($temp)         #Array-ify
    for i in "${arr[@]}"
    do
        if [[ "$i" == *"j-"* ]]; then
            CLUSTER_ID=$(echo "$i" | tr -d '"')

            #print "Spinning up..." $BROWN
            #aws emr wait cluster-running --cluster-id $CLUSTER_ID

            print "Spin up successful; Cluster ID is $CLUSTER_ID" $PURPLE
            return 0
        fi
    done

    print "Spin up unsuccessful; error is above." $RED
    return 1

    exit 0
}

function fun_Obtain_IDs_And_IPs
{
    local OPTIND

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: Obtain_IDs_And_IPs <true/false>"
                print ""
                print "\t If provided with true as the 1st argument, sets internal variables to master node IPs"
                print "\t and saves master and slave ec2 instance ID(s)."
                print "\t Otherwise does nothing."
                return 0
                ;;
            v)  #Verification Mode
                #No verification necessary here...
                return 0
                ;;
        esac
    done

    if [[ "$1" == "true" ]]; then
        #IPs:
        stty -echo
        print "Waiting for master instance to appear..." $BROWN -n

        while :
        do
            MASTER_PRIVATE_IP=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query 'Instances[0].PrivateIpAddress' | tr -d '"')
            if [[ "$MASTER_PRIVATE_IP" == *.*.*.* ]]; then break; fi

            for i in 1 2 3; do print "." $BROWN -n; sleep 0.333; done
        done

        stty echo

        MASTER_PRIVATE_HOSTNAME=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query 'Instances[0].PrivateDnsName' | tr -d '"')
        MASTER_PUBLIC_HOSTNAME=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query 'Instances[0].PublicDnsName' | tr -d '"')
        MASTER_PUBLIC_IP=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query 'Instances[0].PublicIpAddress' | tr -d '"')

        print "\nPrivate: ${MASTER_PRIVATE_IP}; Public: ${MASTER_PUBLIC_IP}" $CYAN

        #IDs:
        stty -echo
        print "Gathering machine EC2 Instance IDs..." $BROWN -n

        MASTER_INSTANCE_ID=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query "Instances[0].Ec2InstanceId" | tr -d '"')
        
        local COUNT=0
        while :
        do
            print "..." $BROWN -n

            local temp_id=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types CORE --query "Instances[$COUNT].Ec2InstanceId" | tr -d '"')
            if [[ "$temp_id" != "i-"* ]]; then break; fi
            SLAVE_INSTANCE_IDS+=($temp_id)

            ((COUNT++))
        done
        
        stty echo

        print "\nMaster ID: ${MASTER_INSTANCE_ID[*]}; Core (Slave) ID(s): ${SLAVE_INSTANCE_IDS[*]}" $CYAN
    fi

    return $?
}

function fun_Configure_File_Processor
{
    local OPTIND

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: Configure_File_Processor \${<key:value pairs array>}"
                print ""
                print "\t Sets key:value replacements for the file processor to use."
                return 0
                ;;
            v)  #Verification Mode (TODO)
                #No verification for now..
                return 0
                ;;
        esac
    done

    replacements="$@"
    print "Replacements set to: $replacements" $PURPLE
}

function fun_psb_Start
{
    local OPTIND

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: psb_Start <temp provision script directory path>"
                print ""
                print "\t If provided with a valid path as the 1st argument, creates a file at the given location"
                print "\t and sets internal variables to point to said file."
                return 0
                ;;
            v)  #Verification Mode
                if [[ ! -d "$2" ]]; then print "Directory provided ($1) is invalid." $RED; return 1; fi
                return 0
                ;;
        esac
    done

    provisionScriptLoc="${1}tmp_provision.sh"
    
    DATE=`date +%Y-%m-%d--%H-%M-%S`
    echo "#!/bin/bash" > $provisionScriptLoc
    echo "# Generated automagically at $DATE" >> $provisionScriptLoc
}

function fun_psb_S3Tarball
{
    local OPTIND

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: psb_S3Tarball <s3 tarball location> <location to untar to>"
                print ""
                print "\t If provided with a valid s3 file as the 1st argument adds a line to the provision script "
                print "\t to untar the specified file to the location in the 2nd argument."
                return 0
                ;;
            v)  #Verification Mode
                #NOTE: No verification on output path what so ever, assuming user can handle that...
                s3Exists $2
                if [[ "$?" == 0 ]]; then print "Specified S3 .tar.gz file does not exist." $RED; return 1; fi
                return 0
                ;;
        esac
    done

    echo "" >> $provisionScriptLoc
    echo "# Untarring tarball from S3..." >> $provisionScriptLoc
    echo "aws s3 cp $1 /tmp/tarball.tar.gz" >> $provisionScriptLoc
    echo "tar -xvzf /tmp/tarball.tar.gz -C $2" >> $provisionScriptLoc
}

function fun_psb_Config_Files
{
    local OPTIND

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: psb_Config_Files <configuration name> <S3 Setup bucket> <array of key-value pairs for configuration files>"
                print ""
                print "\t This function:"
                print "\t\t 1) Adds lines to the provision script to create a directory hierarchy for config files using " -n
                print "the configuration name provided"
                print "\t\t 2) Runs all the configuration files provided (with the exception of the .jar) through the processor"
                print "\t\t 3) Uploads all the files to s3"
                print "\t\t 4) Adds lines to the provision script to copy said files to their respective locations"
                print "\t\t 5) Adds lines to the provision script to configure environment variables to point to the config files"
                print "\n\t Array of key-value pairs should be in key:value format. Supported keys listed below:"
                print "\t\t ( DDL                 :  ddl-> ddl.sql path                          )"
                print "\t\t ( COMMON_RUNTIME_PROP : _common-> common.runtime.properties path     )"
                print "\t\t ( LOG4J2              : _common-> log4j2.xml path                    )"
                print "\t\t ( BROKER_JVM_CONF     :  broker-> jvm.config path                    )"
                print "\t\t ( BROKER_RUNTIME_PROP :  broker-> runtime.properties path            )"
                print "\t\t ( COOR_JVM_CONF       :  coordinator-> jvm.config path               )"
                print "\t\t ( COOR_RUNTIME_PROP   :  coordinator-> runtime.properties path       )"
                print "\t\t ( HIST_JVM_CONF       :  historical-> jvm.config path                )"
                print "\t\t ( HIST_RUNTIME_PROP   :  historical-> runtime.properties path        )"
                print "\t\t ( SPARKLINE_JAR       :  sparkline-> spark-druid-olap-0.x.x.jar path )"
                print "\t\t ( SPARKLINE_PROP      :  sparkline-> sparkline.spark.properties path ) (also goes to thriftserver...)"
                print "\t\t ( INDEXING_JSON       :  indexing-> <name>_index_hadoop.json path    ) (optional)"
                print "\t\t ( MIDDLE_JVM_CONF     :  middleManager-> jvm.config path             ) (optional)"
                print "\t\t ( MIDDLE_RUNTIME_PROP :  middleManager-> runtime.properties path     ) (optional)"
                print "\t\t ( OVER_JVM_CONF       :  overlord-> jvm.config path                  ) (optional)"
                print "\t\t ( OVER_RUNTIME_PROP   :  overlord-> runtime.properties path          ) (optional)"
                print "\n\t File paths can be s3 paths, local paths, or github file URLs"
                print "\t Use rawgit (https://rawgit.com/) for github files"
                print "\t Keys not marked with optional are required; function will not run without them"
                return 0
                ;;
            v)  #Verification Mode
                
                #  This is basically a safety for people using duplicate key value pairs to break the number base verification system
                #  Under base 10, we can deal with up to 9 more argument than are absolutely necessary without the verification being
                #+ compromised; since we have 5 optional arguments this works fine.
                #  11 required arguments, 1 name, 1 S3 setup bucket, 1 verification flag, and 5 optional arguments give us 19.
                if [[ "$#" -gt 19 ]]; then
                    print "Error: arguments are invalid (more supplied than valid options exist)." $RED
                    print "Check for dupicate and invalid keys are try again." $RED
                    return 1;
                fi

                if [[ "$3" != "s3://"* ]]; then print "Error: provide S3 bucket for temp files as 2nd argument." $RED; return 1; fi

                #  Now to check that the flags are provided: a) actually exist & b) include all the required ones
                verif=""
                for pair in "${@:4}"; do #Start at argument 4 to omit the -v flag, the name, and the s3 bucket
                    if [[ "$pair" == *":"* ]]; then
                        key="${pair%%:*}"
                        value="${pair#*:}"
                        
                        case $key in
                            "DDL")                  #Required
                                                    verif=$((100000000000 + verif))
                                                    ;;
                            "COMMON_RUNTIME_PROP")  #Required
                                                    verif=$((10000000000 + verif))
                                                    ;;
                            "LOG4J2")               #Required
                                                    verif=$((1000000000 + verif))
                                                    ;;
                            "BROKER_JVM_CONF")      #Required
                                                    verif=$((100000000 + verif))
                                                    ;;
                            "BROKER_RUNTIME_PROP")  #Required
                                                    verif=$((10000000 + verif))
                                                    ;;
                            "COOR_JVM_CONF")        #Required
                                                    verif=$((1000000 + verif))
                                                    ;;
                            "COOR_RUNTIME_PROP")    #Required
                                                    verif=$((100000 + verif))
                                                    ;;
                            "HIST_JVM_CONF")        #Required
                                                    verif=$((10000 + verif))
                                                    ;;
                            "HIST_RUNTIME_PROP")    #Required
                                                    verif=$((1000 + verif))
                                                    ;;
                            "SPARKLINE_JAR")        #Required
                                                    verif=$((100 + verif))
                                                    ;;
                            "SPARKLINE_PROP")       #Required
                                                    verif=$((10 + verif))
                                                    ;;
                            "INDEXING_JSON")        #Optional
                                                    verif=$((1 + verif))
                                                    ;;
                            "MIDDLE_JVM_CONF")      #Optional
                                                    verif=$((1 + verif))
                                                    ;;
                            "MIDDLE_RUNTIME_PROP")  #Optional
                                                    verif=$((1 + verif))
                                                    ;;
                            "OVER_JVM_CONF")        #Optional
                                                    verif=$((1 + verif))
                                                    ;;
                            "OVER_RUNTIME_PROP")    #Optional
                                                    verif=$((1+ verif))
                                                    ;;
                            *)                      #Invalid
                                                    print "Invalid key \"$key\"" $RED
                                                    ;;
                        esac

                        #Make sure file path is valid (S3/local/Github and exists)
                        copyFile -v $value ${tempLoc}$key
                        if [[ "$?" != 0 ]]; then print "Invalid file path \"$value\"." $RED; return 1; fi

                    else
                        print "Error: key-value pair \"$pair\" is formatted incorrectly.." $RED
                        return 1
                    fi

                done

                if [[ "$verif" -lt 111111111111 ]]; then 
                    print "Error: Missing required paths (Verification code is $verif). Make sure configuration name is 1st argument." $RED
                    return 1
                fi

                return 0
                ;;
        esac
    done

    # Add line to provision script to setup directory hierarchy..
    echo "" >> $provisionScriptLoc
    echo "# Config Files" >> $provisionScriptLoc
    echo "mkdir -p /home/hadoop/configurations/$1/{ddl,druid/{_common,broker,coordinator,historical,middleManager,overlord},indexing,sparkline}" >> $provisionScriptLoc

    # Just in case..
    aws s3 mb $2

    # Process config files..
    for pair in "${@:3}"; do #Start at argument 3 to omit the name and the s3 bucket
        if [[ "$pair" == *":"* ]]; then
            key="${pair%%:*}"
            value="${pair#*:}"

            copyFile $value ${tempLoc}$key
            if [[ $key != "SPARKLINE_JAR" ]]; then fileProcessor ${tempLoc}$key ${replacements[@]}; fi
            copyFile ${tempLoc}$key ${2}$key

            finalPath="/home/hadoop/configurations/$1"
            case $key in
                "DDL")                  #Required
                                        finalPath="$finalPath/ddl/ddl.sql"
                                        ;;
                "COMMON_RUNTIME_PROP")  #Required
                                        finalPath="$finalPath/druid/_common/common.runtime.properties"
                                        ;;
                "LOG4J2")               #Required
                                        finalPath="$finalPath/druid/_common/log4j2.xml"
                                        ;;
                "BROKER_JVM_CONF")      #Required
                                        finalPath="$finalPath/druid/broker/jvm.config"
                                        ;;
                "BROKER_RUNTIME_PROP")  #Required
                                        finalPath="$finalPath/druid/broker/runtime.properties"
                                        ;;
                "COOR_JVM_CONF")        #Required
                                        finalPath="$finalPath/druid/coordinator/jvm.config"
                                        ;;
                "COOR_RUNTIME_PROP")    #Required
                                        finalPath="$finalPath/druid/coordinator/runtime.properties"
                                        ;;
                "HIST_JVM_CONF")        #Required
                                        finalPath="$finalPath/druid/historical/jvm.config"
                                        ;;
                "HIST_RUNTIME_PROP")    #Required
                                        finalPath="$finalPath/druid/historical/runtime.properties"
                                        ;;
                "SPARKLINE_JAR")        #Required
                                        finalPath="$finalPath/sparkline/spark-druid-olap-assembly.jar"
                                        ;;
                "SPARKLINE_PROP")       #Required
                                        finalPath="$finalPath/sparkline/sparkline.spark.properties"
                                        ;;
                "INDEXING_JSON")        #Optional
                                        finalPath="$finalPath/indexing/index_hadoop.json"
                                        ;;
                "MIDDLE_JVM_CONF")      #Optional
                                        finalPath="$finalPath/druid/middleManager/jvm.config"
                                        ;;
                "MIDDLE_RUNTIME_PROP")  #Optional
                                        finalPath="$finalPath/druid/middleManager/runtime.properties"
                                        ;;
                "OVER_JVM_CONF")        #Optional
                                        finalPath="$finalPath/druid/overlord/jvm.config"
                                        ;;
                "OVER_RUNTIME_PROP")    #Optional
                                        finalPath="$finalPath/druid/overlord/runtime.properties"
                                        ;;
                *)                      #Invalid
                                        print "Error: Sending software developer a howler.. ($key)" $RED
                                        ;;
            esac

            echo "aws s3 cp ${2}$key $finalPath" >> $provisionScriptLoc
        fi
    done

    # Other Config File Related Things:
    echo "" >> $provisionScriptLoc
    echo "# Other Config File Related Things" >> $provisionScriptLoc
    echo "echo \"\"" >> $provisionScriptLoc

cat << EOF >> $provisionScriptLoc

#Technically the next three lines should only run on the master node, but they'll fail safely on the slaves.
echo -e "\n#--------------------------------------------------------------------------------" > /tmp/middlePart
echo "# From other config file" >> /tmp/middlePart
cat /usr/lib/spark/conf/spark-defaults.conf /tmp/middlePart /home/hadoop/configurations/${1}/sparkline/sparkline.spark.properties > /tmp/concat
mv /tmp/concat /home/hadoop/configurations/${1}/sparkline/sparkline.spark.properties
cat /home/hadoop/bash_additions >> /home/hadoop/.bashrc
rm /home/hadoop/bash_additions
EOF

    echo "echo \"# Config Vars\" >> /home/hadoop/.bashrc" >> $provisionScriptLoc
    echo "echo \"export CONFIG_LOCATION=/home/hadoop/configurations/$1\" >> /home/hadoop/.bashrc" >> $provisionScriptLoc
    echo "echo \"export DRUID_ZK_HOST=$MASTER_PRIVATE_IP\" >> /home/hadoop/.bashrc" >> $provisionScriptLoc

    print "Config Files added to psb." $PURPLE
    return 0
}

function fun_psb_Misc
{
    local OPTIND

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: psb_Misc <true/false>"
                print ""
                print "\t If provided with true as the 1st argument, does the following:"
                print "\t\t 1) Makes /{mnt,mnt1}/druid/indexCache folders"
                # print "\t\t 2) Copies thriftserver scripts to /usr/lib/spark/sbin"
                print "\t Otherwise does nothing."
                return 0
                ;;
            v)  #Verification Mode
                #No verification necessary here...
                return 0
                ;;
        esac
    done

    if [[ "$1" == "true" ]]; then
        echo -e "\n# Misc Things" >> $provisionScriptLoc
        echo "mkdir -p /{mnt,mnt1}/druid/indexCache" >> $provisionScriptLoc
        # echo "mv /home/hadoop/start-thrift /usr/lib/spark/sbin/start-sparklinedatathriftserver.sh" >> $provisionScriptLoc
        # echo "mv /home/hadoop/stop-thrift  /usr/lib/spark/sbin/stop-sparklinedatathriftserver.sh"  >> $provisionScriptLoc
        # echo "chown spark /usr/lib/spark/sbin/" >> $provisionScriptLoc
        # #May need to change permissions somewhere here..
    fi

    return 0
}

function fun_EMR_Wait
{
    local OPTIND

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: EMR_Wait <true/false>"
                print ""
                print "\t If provided with true as the first argument, stalls until aws emr wait allows us to continue."
                print "\t In other words waits until machines are fully spun up."
                return 0
                ;;
            v)  #Verification Mode
                #No real verification here; probably not necessary.
                return 0
                ;;
        esac
    done

    if [[ "$1" == "true" ]]; then
        print "Waiting for all nodes to fully spin up (this might take a while)..." $BROWN
        aws emr wait cluster-running --cluster-id $CLUSTER_ID
        print "Cluster fully spun up!" $CYAN
    fi
}

function fun_psb_Run
{
    local OPTIND

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: psb_Run <S3 Setup Bucket>"
                print ""
                print "\t Uploads the script that has been generated using the psb"
                print "\t (provison script builder) methods thus far and runs it on"
                print "\t all instances in the cluster."
                return 0
                ;;
            v)  #Verification Mode
                #No verification necessary here...
                return 0
                ;;
        esac
    done

    echo -e "\ntouch /tmp/psf-was-here" >> $provisionScriptLoc #psf = provision script file
    echo "#-------------------------------------------------------------------------------" >> $provisionScriptLoc

    print "Uploading provision script to ${1}psf.sh..." $PURPLE
    aws s3 cp $provisionScriptLoc "${1}psf.sh"

    print "Running provision script on all instances..." $CYAN
    ec2Run "ms" "${1}psf.sh" "hadoop" "/home/hadoop" "PSF"

    return $?
}

function fun_Start_Master
{
    local OPTIND

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: Start_Master <S3 Setup Bucket>"
                print ""
                print "\t Uploads a startup script (OCB + ZK Server + Thriftserver)"
                print "\t and runs it on the master instance."
                return 0
                ;;
            v)  #Verification Mode
                #No verification necessary here...
                return 0
                ;;
        esac
    done

    local scriptLoc=${tempLoc}start-master

    echo "#!/bin/bash" > $scriptLoc
    
    echo "while [[ ! -f /tmp/psf-was-here ]]; do :; done" >> $scriptLoc
    echo "rm /tmp/psf-was-here" >> $scriptLoc
    
    echo "source /home/hadoop/.bashrc" >> $scriptLoc
    
    echo "bash /home/hadoop/druid/druid-0.9.0/stop-all.sh; cd /home/hadoop/druid/zookeeper-3.4.6/bin && ./zkServer.sh stop && cd -" >> $scriptLoc
    echo "cd /usr/lib/spark/sbin && sudo -u spark ./stop-sparklinedatathriftserver.sh && cd " >> $scriptLoc
    
    echo "sudo curl https://raw.githubusercontent.com/SparklineData/spark-druid-olap/master/scripts/start-sparklinedatathriftserver.sh -o /usr/lib/spark/sbin/start-sparklinedatathriftserver.sh" >> $scriptLoc
    echo "sudo curl https://raw.githubusercontent.com/SparklineData/spark-druid-olap/master/scripts/stop-sparklinedatathriftserver.sh -o /usr/lib/spark/sbin/stop-sparklinedatathriftserver.sh" >> $scriptLoc
    
    echo "sudo chmod +x /usr/lib/spark/sbin/*thriftserver.sh" >> $scriptLoc
    echo "sudo chown -R hadoop /usr/lib/spark/sbin" >> $scriptLoc
    echo "sudo chown -R hadoop /var/log/spark" >> $scriptLoc

    echo "cd /home/hadoop/druid/zookeeper-3.4.6/bin && ./zkServer.sh start && cd - && cd /home/hadoop/druid/druid-0.9.0/ && bash start-masters-hadoop.sh && cd -" >> $scriptLoc
    
    echo "sudo -u hadoop spark-sql --jars \${CONFIG_LOCATION}/sparkline/spark-druid-olap-assembly.jar --packages com.databricks:spark-csv_2.10:1.1.0,SparklineData:spark-datetime:0.0.2  -f \${CONFIG_LOCATION}/ddl/ddl.sql" >> $scriptLoc

    echo "cd /usr/lib/spark/sbin && sudo -u hadoop ./start-sparklinedatathriftserver.sh \$CONFIG_LOCATION/sparkline/spark-druid-olap-assembly.jar --master yarn --properties-file \$CONFIG_LOCATION/sparkline/sparkline.spark.properties && cd -" >> $scriptLoc

    aws s3 cp $scriptLoc "${1}start-master.sh"
    ec2Run "m" "${1}start-master.sh" "hadoop" "/home/hadoop" "Start-Master" #Running as Hadoop instead of Spark because of Environment Variables

    print "Master Instance Services are starting..." $CYAN
}

function fun_Start_Slaves
{
    local OPTIND

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: Start_Slaves <S3 Setup Bucket>"
                print ""
                print "\t Uploads a startup script (Historicals)"
                print "\t and runs it on the slave instances."
                return 0
                ;;
            v)  #Verification Mode
                #No verification necessary here...
                return 0
                ;;
        esac
    done

    local scriptLoc=${tempLoc}start-slaves

    echo "#!/bin/bash" > $scriptLoc
    echo "while [[ ! -f /tmp/psf-was-here ]]; do :; done" >> $scriptLoc
    echo "rm /tmp/psf-was-here" >> $scriptLoc
    echo "source /home/hadoop/.bashrc" >> $scriptLoc
    echo "bash /home/hadoop/druid/druid-0.9.0/stop-all.sh" >> $scriptLoc
    echo "cd /home/hadoop/druid/druid-0.9.0/ && bash start-historical-hadoop.sh && cd -" >> $scriptLoc

    aws s3 cp $scriptLoc "${1}start-slaves.sh"
    ec2Run "s" "${1}start-slaves.sh" "hadoop" "/home/hadoop" "Start-Slaves"

    print "Slave Instance Services are starting..." $CYAN
}

function fun_Add2Hosts
{
    local OPTIND

    while getopts ":hv" flag; do
        case $flag in
            h)  #Prints help text
                print "usage: Add2Hosts <true/false> <nameOfConfig>"
                print ""
                print "\t If provided with true as the 1st argument, adds ips of all instances in cluster to /etc/hosts (requests sudo)."
                print "\t Second argument is optional (used for a /etc/hosts comment)."
                print "\t Otherwise does nothing."
                return 0
                ;;
            v)  #Verification Mode
                #No verification necessary here...
                return 0
                ;;
        esac
    done

    if [[ "$1" == "true" ]]; then
        echo "" >> /etc/hosts
        echo "# EMR Instances in $CLUSTER_ID ($2)" >> /etc/hosts

        stty -echo
        print "Adding to /etc/hosts..." -n

        COUNT=0
        while :
        do
            publicIp=$(aws emr list-instances --cluster-id $CLUSTER_ID --query "Instances[$COUNT].PublicIpAddress" | tr -d '"')
            if [[ "$publicIp" != *.*.*.* ]]; then break; fi
            privateName=$(aws emr list-instances --cluster-id $CLUSTER_ID --query "Instances[$COUNT].PrivateDnsName" | tr -d '"')

            echo $publicIp $privateName | tr -d '"' >> /etc/hosts

            print "..." -n
            ((COUNT++))
        done

        stty echo
        print "\nAdded $COUNT instance(s) to /etc/hosts." $CYAN
    fi

    return 0
}

function processOptions
{
    for i in "${options[@]}"
    do
        #Get the variable to be set..
        eval temp=\$"{$i[@]}"
        while [ -z "$temp" ]; do
            print "The variable for $i is not set." $BROWN
            
            if [[ "$interactive" != "true" ]]; then exit 1; fi #If interact is set to false, die.

            eval "fun_$i -h"                                   #Run function for opt, with -h (help text)
            print "Enter arguments for $i:"

            read input
            eval "$i=\$input"                                  #Set the option var (derefed $i) to the input
            eval temp=\$"{$i[@]}"                              #Again, to make this a do-while loop
            changed="true"
        done

        #Get the variable approved (verified)..
        eval "fun_$i -v \${$i[@]}"                             #Run the function, but with -v and the corresponding variable as an argument.
        temp=$?
        until [ $temp -eq 0 ]; do 
            print "The variable for $i is set incorrectly." $BROWN

            if [[ "$interactive" != "true" ]]; then exit 1; fi #If interact is set to false, die.

            eval "fun_$i -h"                                   #Run function for opt, with -h (help text)
            eval temp="\${$i[@]}"
            print "Old arguments were: " -n && print " $temp" $RED
            print "Enter new arguments for $i:"

            read input
            eval "$i=\$input"
            
            eval "fun_$i -v \${$i[@]}"                         #Let's try again..
            temp=$?
            changed="true"
        done

        #Now the call the option function normally with its (verified!) variable..
        eval "fun_$i \${$i[@]}"

        if [ "$?" -ne 0 ]; then
            print "Error(s) occurred; exiting." $RED
            exit 1
        fi
    done
}

function createConfigFile
{
    DATE=`date +%Y-%m-%d--%H-%M-%S`
    FILE="configFile-"$DATE
    touch "$FILE"

    echo "# Auto generated on $DATE." >> $FILE

    for i in "${options[@]}"
    do
        eval temp="\${$i[@]}"
        echo "$i=\"$temp\"" >> $FILE
    done

    print "Created config file named $FILE in $(pwd)"
}

function fin
{
    print "Setup completed." $PURPLE
    print "$MASTER_PUBLIC_IP $MASTER_PRIVATE_HOSTNAME" $BOLD
    print "Here are the steps that were (successfully) run:"

    for i in "${options[@]}"
    do
        print "\t $i"
    done

    if [[ $changed == "true" ]]; then
        print "\nOptions were changed during setup; Save changes to new config file? (enter option #)"

        select yn in "Yes" "No"; do
            case $yn in
                Yes )  createConfigFile; break;;
                No  )  break;;
            esac
        done
    fi

    tput bel
    print "Fin." $BOLD
}

function helpText
{

cat << EOF
usage: sudo ./spinup.sh [-i | -q | -Q] [-D] [-c <config file>]
 
    -i                  Interactive Mode: Prompts user for settings.
    -q                  Quiet Mode: Omits the fallback to interactive mode.
    -Q                  QuietQuiet Mode: Same as quiet mode, but with no text output. (Kinda broken, use > /dev/null 2>&1 instead)
    -D                  Debug Mode: Internally runs set -x (prints out all commands run, line by line)
    -c <config file>    Config File Mode: Sets params based on config file, falls back to interactive mode.
EOF

exit $1

}

function emergencyExit
{
    stty echo
    print "\nSpinup incomplete; Are you sure you wish to exit? (enter option #)" $RED
    
    select yn in "Yes" "No"; do
        case $yn in
            Yes )  exit 1; break;;
            No  )  return;;
        esac
    done

    # Failsafe exit actions go here:
    stty echo
}

trap emergencyExit SIGINT SIGTERM

processArguments "$*"
checkDependencies
processOptions
fin

##########################
# AUTHOR:  Rahul Butani  #
# DATE:    July 22, 2016 #
# VERSION: 0.3.3         #
##########################