# Stream Writer

## Overview
This python project writes to event hub stream.

## Get Started
Launch the dev container using `ctrl + shift + p` and choosing `rebuild without cache and repoen in container`
Once the dev container spins up do these steps
1. sudo apt install python3.10-venv
2. python3 -m venv venv
3. source venv/bin/activate
4. pip install -r requirements.txt 
5. az login --use-device
6. Go inside src/ and run `python3 write-stream.py`