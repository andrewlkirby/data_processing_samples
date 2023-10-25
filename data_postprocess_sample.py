import json
import shelve
from typing import Dict, Optional, Any
from pathlib import Path
import pandas as pd
from dataclasses import dataclass, field
import os
import boto3
import re
from tqdm.notebook import tqdm
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

s3 = boto3.client(
                  's3', 
                  aws_access_key_id = os.getenv('some_aws_access_key'), 
                  aws_secret_access_key = os.getenv('some_aws_secret_key')
                  )
BUCKET = 'some-aws-bucket'

@dataclass
class RegionShapeAttributes:
    cx: int = 0
    cy: int = 0
    name: str = "point"
    # conf: float = 0

@dataclass
class Post:
    filename: str = ''
    file_size: int = 0
    region_shape_attributes: RegionShapeAttributes = field(default_factory = RegionShapeAttributes)
    file_attributes: dict = field(default_factory = dict) 
    region_count: int = 0
    region_id: int = 0
    region_attributes: dict = field(default_factory = dict)

    @staticmethod
    def load_json(input_json: Path) -> Dict[str, Any]:
        with open(input_json, 'r') as j:
            return json.loads(j.read())
    
    @staticmethod
    def check_metadata(key: str) -> Optional[int]:
        path = 'metadata'
        if Path(f"{path}.db").is_file():
            with shelve.open(path) as d:
                if key in d: return d[key]
                else: return None
        else: return None

    @staticmethod
    def update_metadata(key, file_size):
        path = 'metadata'
        with shelve.open(path) as d:
            d[key] = file_size

    def get_file_size(self, s3_key: Path) -> int:
        match = re.search(r'(\.com\/)(.*?\.jpg)', s3_key)
        key = match.group(2)
        if Path(key).suffix != '.jpg': 
            print(f'My regular expression failed on this s3 key: {s3_key}')
            return 0
        file_size = self.check_metadata(key)
        if file_size: return file_size
        else:
            # Get the object metadata from S3.
            try:
                object_metadata = s3.get_object(Bucket = BUCKET, Key = key)
            except:
                space_key = key.replace('%20', ' ')
                object_metadata = s3.get_object(Bucket = BUCKET, Key = space_key)
            # Get the size of the image file from the object metadata.
            file_size = object_metadata['ContentLength']
            self.update_metadata(key, file_size)
            return file_size
    
    @staticmethod
    def get_file_name(s3_key: Path) -> str:
        file_name = Path(re.findall(r'.*\.jpg', s3_key)[0]).name
        space_name = file_name.replace('%20', ' ')
        return space_name
    
    @staticmethod
    def get_reg_sh_atts(tool: Dict[str, Any]) -> RegionShapeAttributes:
        reg_sh_atts = RegionShapeAttributes()
        reg_sh_atts.cx += int(tool['point'][0])
        reg_sh_atts.cy += int(tool['point'][1])
        return reg_sh_atts

    def process(self, ango_data: Dict[str, Any]) -> pd.DataFrame:
        data = []
        for asset in ango_data:
            for tool in tqdm(asset['task']['tools']):
                data_item = Post()
                tool_s3_key = asset['dataset'][tool['page']]
                data_item.filename = self.get_file_name(tool_s3_key)
                data_item.file_size += self.get_file_size(tool_s3_key)
                data_item.region_shape_attributes = self.get_reg_sh_atts(tool)
                data_item.region_attributes = json.dumps({'type': tool['title']})
                data.append(data_item)
        df = pd.DataFrame(data)
        df['region_shape_attributes'] = [json.dumps(item) for item in df['region_shape_attributes'].to_list()]
        # Order columns in a specific way:
        df = df[['filename', 
                'file_size', 
                'file_attributes', 
                'region_count', 
                'region_id', 
                'region_shape_attributes', 
                'region_attributes'
                ]
                ]
        return df
    
    def test(self, ango_json: Path) -> pd.DataFrame:
        ango_data = self.load_json(ango_json)
        df = self.process(ango_data)
        df.to_csv(f'postprocess_{str(datetime.now()).replace(" ", "_")}.csv', index = False)