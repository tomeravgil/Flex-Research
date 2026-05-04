import requests
import json
import random

def get_random_paper(filepath):
    with open(filepath, 'r') as f:
        lines = f.readlines()
    line = random.choice(lines)
    paper = json.loads(line)
    return paper

def get_paper_prestige(paper_title):
    url = f"https://api.openalex.org/works?search={paper_title}"
    response = requests.get(url).json()
    
    if not response.get('results'):
        return "Paper not found", 0.2

    paper = response['results'][0]

    print(paper.get('fwci'))
    
    primary_loc = paper.get('primary_location') or {}
    source = primary_loc.get('source')
    fwci = primary_loc.get('fwci')
    print(fwci)

    if source is None:
        venue_name = "ArXiv / Pre-print"
        score = 0.2
    else:
        venue_name = source.get('display_name', "Unknown Venue")
        
        prestige_map = {
            "Neural Information Processing Systems": 1.0,
            "International Conference on Machine Learning": 1.0,
            "Computer Vision and Pattern Recognition": 1.0,
            "International Conference on Learning Representations": 1.0
        }
        
        score = 0.2
        for venue, weight in prestige_map.items():
            if venue.lower() in venue_name.lower():
                score = weight
                break
            
    return venue_name, score


if __name__ == "__main__":
    data_path = "../kaggle/data/arxiv-metadata-oai-snapshot.json"
    
    paper = get_random_paper(data_path)
    title = paper.get('title', '').replace('\n', ' ').strip()
    
    print(f"Random paper: {title}")
    print("-" * 60)
    
    venue, rank_weight = get_paper_prestige(title)
    print(f"Venue: {venue} | Weight: {rank_weight}")