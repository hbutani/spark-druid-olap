# Spinup Script

[Sample spinup configurations](/configurations) and [configuration files](/tpch1_configFiles) provided.

### Before running:

1. Ensure AWS CLI tools are installed and configured _(aws configure)_ for your AWS account and text output.
2. Download spinup tarball (available [here](https://s3.amazonaws.com/emr-sparkline/emr-sparkline-spinup.tar.gz)) and save it to your S3 at _s3://emr-sparkline/emr-sparkline-spinup.tar.gz_ or a custom location.
  - If you saved to a custom location, set it in the spinup config file.
3. Set your security groups and key pair in the config file.

### To run:

```bash
# In this directory:
sudo ./spinup -c configurations/configFile-TPCHSample
```

Tested on bash v3.2+ (Mac OS X and Linux). Good Luck!
