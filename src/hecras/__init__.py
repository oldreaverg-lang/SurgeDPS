"""
HEC-RAS 2D Integration Module (Tier 3 Flood Modeling)

Provides premium-tier compound flood modeling using HEC-RAS 6.5+
native Linux compute engines running on AWS Batch Fargate Spot.

Modules:
    config          — Configuration for HEC-RAS templates and paths
    template_gen    — Generate HEC-RAS project files from templates
    boundary_injector — Inject storm-specific boundary conditions
    runner          — Execute HEC-RAS and manage lifecycle
    result_extractor — Extract flood depth grids from HDF5 output
    synthetic_results — Generate synthetic results for dev/testing
"""
