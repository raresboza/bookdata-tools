stage ClusterStats {
    cmd "jupytext --to ipynb --execute ClusterStats.py"
    dep "ClusterStats.py"
    dep "book-links/cluster-stats.parquet"
    out -nocache "ClusterStats.ipynb"
}

stage LinkageStats {
    cmd "jupytext --to ipynb --execute LinkageStats.py"
    dep "LinkageStats.py"
    dep "book-links/gender-stats.csv"
    out -nocache "LinkageStats.ipynb"
    metric "book-coverage.json"
}

stage html-report {
    target LinkageStats
    target ClusterStats

    cmd "jupyter nbconvert --to html \${item}.ipynb"
    dep "\${item}.ipynb"
    out "\${item}.html"
}
