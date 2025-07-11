from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import asyncio
from data_fetcher import pipeline_loop
#from pipeline.data_fetcher import pipeline_loop  # rename this if needed

app = FastAPI()

@app.on_event("startup")
async def start_pipeline():
    loop = asyncio.get_event_loop()
    loop.create_task(pipeline_loop())  # background prediction pipeline

@app.get("/", response_class=HTMLResponse)
async def root():
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>UPI Fraud Detection System</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            body {
                background-color: #f8f9fa;
                display: flex;
                align-items: center;
                justify-content: center;
                height: 100vh;
                margin: 0;
                font-family: 'Segoe UI', sans-serif;
            }
            .container {
                background: white;
                padding: 2rem 3rem;
                border-radius: 1rem;
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                text-align: center;
            }
            h1 {
                font-size: 2.5rem;
                margin-bottom: 1rem;
            }
            .lead {
                font-size: 1.25rem;
                color: #6c757d;
            }
            .btn-primary {
                margin-top: 1.5rem;
                padding: 0.75rem 1.5rem;
                font-size: 1.1rem;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🛡️ UPI Fraud Detection Pipeline</h1>
            <p class="lead">Monitoring real-time UPI transactions and flagging fraudulent activity with live alerts.</p>
            <a href="https://app.powerbi.com/view?r=eyJrIjoiMWMxNmViNjgtM2Q4My00NDZjLTkxOWItODRiMTdmZGE0MTNkIiwidCI6IjVlOGZlMWVlLTg5NmYtNDdjZi1iMjFjLTUyMWMxMmJmODViYiJ9"
               target="_blank" 
               class="btn btn-primary">
               📊 View Dashboard
            </a>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)
