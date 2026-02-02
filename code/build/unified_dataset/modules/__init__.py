"""
Unified Dataset Modules

This package contains modules for creating a unified policy papers dataset
from multiple sources (OpenAlex, Semantic Scholar, NBER).
"""

from .data_loader import load_all_sources, standardize_dataframe
from .matcher import match_papers, create_match_registry
from .merger import merge_papers, resolve_conflicts
from .coverage_analyzer import analyze_coverage, generate_hypotheses
from .report_generator import generate_all_reports
