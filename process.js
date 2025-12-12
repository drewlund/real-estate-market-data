name: Update Redfin Data

on:
  schedule:
    # Runs every Thursday at 6 AM UTC (Redfin updates Wednesdays)
    - cron: '0 6 * * 4'
  workflow_dispatch: # Allows manual trigger

jobs:
  update-data:
    runs-on: ubuntu-latest
    
    permissions:
      contents: write
      pages: write
      id-token: write
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
      
      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '20'
      
      - name: Run processing script
        run: node process.js
      
      - name: Upload Pages artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: ./output
      
      - name: Deploy to GitHub Pages
        uses: actions/deploy-pages@v4
