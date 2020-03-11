# charm-skelton

This is the skeleton of an [operator framework](https://github.com/canonical/operator) k8s charm.

## Usage

To create a charm based on this skeleton:

```bash
# Download the skeleton from github
wget https://github.com/charmed-osm/charm-skeleton-k8s/archive/master.zip

# Unpack the archive
unzip master.zip
mv charm-skeleton-k8s-master mycharm
cd mycharm

# Initialize the git repo
git init

# Install the submodules
git submodule add https://github.com/canonical/operator mod/operator
git submodule update --init

# Edit metadata.yaml: set the name and describe your charm
vim metadata.yaml
[...]

# Commit your changes
git add .
git commit -a
```

To deploy charm to juju:

```bash
juju deploy .

# Make sure the charm is in an Active state
juju status
```

