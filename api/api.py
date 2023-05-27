import pandas as pd
from pycaret.time_series import load_model, predict_model
from fastapi import FastAPI
import uvicorn
from pydantic import BaseModel

# Define a request body
class Item(BaseModel):
    steps: int

# Create the app object
app = FastAPI()

# Load trained Pipeline
model = load_model('api/20230527213535_model')

@app.post('/predict')
def predict(item: Item):
    # Use the model to make a prediction
    predictions = predict_model(model, fh=item.steps) 
    
    last_prediction = predictions['y_pred'][-1]
    return {'prediction': float(last_prediction)}


if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)
