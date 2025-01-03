def is_mpi_affiliation(affiliation: str) -> bool:
    """Check if the affiliation belongs to the Max-Planck Institute."""
    return any(keyword in affiliation for keyword in ['Max Planck', 'Max Plank']) or \
            any(keyword in affiliation.replace(' ', '') for keyword in ['Max-Planck'])
