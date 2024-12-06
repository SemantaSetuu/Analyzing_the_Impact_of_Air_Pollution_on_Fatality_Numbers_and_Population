import os
from flask import Flask, render_template_string, send_file, request
import pandas as pd
from sqlalchemy import create_engine

# Initialize the Flask app
app = Flask(__name__)

# Database connection URI
DB_URI = "postgresql://postgres:BroMe@localhost:5432/dagster"
engine = create_engine(DB_URI)

# Load data from the PostgreSQL table
query = 'SELECT * FROM "Graph"'
paths = pd.read_sql(query, engine)

# Define the route to display the graphs
@app.route('/', methods=['GET'])
def index():
    search_query = request.args.get('search', '').lower()
    
    # Filter graphs based on search query
    if search_query:
        filtered_paths = paths[paths['graph_name'].str.lower().str.contains(search_query)]
    else:
        filtered_paths = paths

    # Convert DataFrame to list of tuples for easier rendering in Jinja
    graphs = filtered_paths[['graph_name', 'graph_path']].to_records(index=False)
    
    # HTML template to display images
    html_template = """
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Graphs Display</title>
        <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css" rel="stylesheet">
        <script src="https://code.jquery.com/jquery-3.5.1.slim.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@4.5.2/dist/js/bootstrap.bundle.min.js"></script>
    </head>
    <body>
        <div class="container">
            <h1 class="my-4 text-center">Graphs Display</h1>
            
            <form method="get" class="form-inline justify-content-center mb-4">
                <input type="text" name="search" class="form-control mr-2" placeholder="Search by graph name" value="{{ request.args.get('search', '') }}">
                <button type="submit" class="btn btn-primary">Search</button>
            </form>
            
            <div class="row">
                {% for graph_name, graph_path in graphs %}
                    <div class="col-lg-4 col-md-6 mb-4">
                        <div class="card">
                            <img src="/graph/{{ loop.index0 }}" class="card-img-top" alt="{{ graph_name }}" style="height: 300px; object-fit: cover;">
                            <div class="card-body text-center">
                                <h5 class="card-title">{{ graph_name }}</h5>
                                <button class="btn btn-primary" data-toggle="modal" data-target="#graphModal{{ loop.index0 }}">View Graph</button>
                            </div>
                        </div>
                    </div>
                    
                    <!-- Modal for graph -->
                    <div class="modal fade" id="graphModal{{ loop.index0 }}" tabindex="-1" aria-labelledby="graphModalLabel{{ loop.index0 }}" aria-hidden="true">
                        <div class="modal-dialog modal-lg">
                            <div class="modal-content">
                                <div class="modal-header">
                                    <h5 class="modal-title" id="graphModalLabel{{ loop.index0 }}">{{ graph_name }}</h5>
                                    <button type="button" class="close" data-dismiss="modal" aria-label="Close">
                                        <span aria-hidden="true">&times;</span>
                                    </button>
                                </div>
                                <div class="modal-body">
                                    <img src="/graph/{{ loop.index0 }}" alt="{{ graph_name }}" class="img-fluid">
                                </div>
                                <div class="modal-footer">
                                    <button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>
                                </div>
                            </div>
                        </div>
                    </div>
                {% endfor %}
            </div>
        </div>
    </body>
    </html>
    """
    
    return render_template_string(html_template, graphs=graphs)

# Define the route to serve individual graph images
@app.route('/graph/<int:graph_id>')
def graph(graph_id):
    # Make sure graph_id is within the bounds of the DataFrame
    if 0 <= graph_id < len(paths):
        # Get the graph path from the DataFrame
        graph_path = paths.iloc[graph_id]['graph_path']
        
        # Check if the file exists
        if os.path.exists(graph_path):
            return send_file(graph_path, mimetype='image/png')
        else:
            return "Graph file not found", 404
    else:
        return "Invalid graph ID", 404

# Run the Flask app
if __name__ == "__main__":
    app.run(debug=True)
