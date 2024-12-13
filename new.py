import re

# Sample HTML content
html_content = '<td itemprop="datePublished">Jun 29, 2004</td>'

# Regular expression to extract the year
year = re.search(r'\d{4}', html_content).group()

print(year)
