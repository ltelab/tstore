[![lint](https://github.com/ltelab/tstore/actions/workflows/lint.yaml/badge.svg)](https://github.com/ltelab/tstore/actions/workflows/lint.yaml)
[![ci](https://github.com/ltelab/tstore/actions/workflows/ci.yaml/badge.svg)](https://github.com/ltelab/tstore/actions/workflows/ci.yaml)
[![GitHub license](https://img.shields.io/github/license/ltelab/tstore.svg)](https://github.com/ltelab/tstore/blob/main/LICENSE)

# TStore

Flexible storage for time series

## Requirements

- [mamba](https://github.com/mamba-org/mamba), which can be installed using conda or [mambaforge](https://github.com/conda-forge/miniforge#mambaforge) (see [the official installation instructions](https://github.com/mamba-org/mamba#installation))
- [snakemake](https://snakemake.github.io), which can be installed using [conda or mamba](https://snakemake.readthedocs.io/en/stable/getting_started/installation.html)

## Instructions

1. Create a conda environment:

```bash
snakemake -c1 create_environment
```

2. Activate it (if using conda, replace `mamba` for `conda`):

```bash
mamba activate tstore
```

3. Register the IPython kernel for Jupyter:

```bash
snakemake -c1 register_ipykernel
```

4. Activate pre-commit for the git repository:

```bash
pre-commit install
pre-commit install --hook-type commit-msg
```

## Acknowledgments

- Based on the [cookiecutter-data-snake :snake:](https://github.com/martibosch/cookiecutter-data-snake) template for reproducible data science.
