from graphviz import Digraph

# Create a new directed graph
dot = Digraph(comment='Retail Sales ETL Pipeline Architecture')
dot.attr(rankdir='LR')

# Add nodes
dot.attr('node', shape='cylinder')
dot.node('CSV', 'Raw Sales\nData (CSV)')
dot.node('PS', 'PostgreSQL DB')
dot.node('DW', 'Data\nWarehouse')

dot.attr('node', shape='box')
dot.node('DAG', 'Airflow DAG\n(retail_etl_dag.py)')
dot.node('STG', 'Staging Layer\n(stg_sales)')
dot.node('DIM', 'Dimension Tables\n(customers, products)')
dot.node('FACT', 'Fact Table\n(sales)')

# Add subgraphs
with dot.subgraph(name='cluster_0') as c:
    c.attr(label='Apache Airflow')
    c.attr('node', style='filled', color='lightpink')
    c.node('DAG')

with dot.subgraph(name='cluster_1') as c:
    c.attr(label='dbt Transformation')
    c.attr('node', style='filled', color='lightblue')
    c.node('STG')
    c.node('DIM')
    c.node('FACT')

# Add edges
dot.edge('CSV', 'PS', 'Load')
dot.edge('PS', 'STG', 'Raw Data')
dot.edge('STG', 'DIM', 'Transform')
dot.edge('STG', 'FACT', 'Transform')
dot.edge('DIM', 'DW', 'Load')
dot.edge('FACT', 'DW', 'Load')
dot.edge('DAG', 'PS', 'Orchestrate')
dot.edge('DAG', 'STG', 'Trigger dbt')

# Save the diagram
dot.render('docs/architecture', format='png', cleanup=True)