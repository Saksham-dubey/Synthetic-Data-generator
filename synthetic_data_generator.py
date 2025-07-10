from faker import Faker
import pandas as pd
import random
import datetime
import uuid
import hashlib
class HierarchicalDataGenerator:
    def __init__(self):
        self.fake = Faker()
        # Store generated values for hierarchical relationships
        self.hierarchy_cache = {}
        self.id_counter = 0  # For sequential IDs
        self.used_ids = set()  # To track used IDs and ensure uniqueness
        
        # Comprehensive mapping of countries, states, and cities
        self.location_hierarchy = {
            'United States': {
                'California': ['Los Angeles', 'San Francisco', 'San Diego', 'San Jose', 'Sacramento'],
                'New York': ['New York City', 'Buffalo', 'Albany', 'Rochester', 'Syracuse'],
                'Texas': ['Houston', 'Austin', 'Dallas', 'San Antonio', 'Fort Worth'],
                'Florida': ['Miami', 'Orlando', 'Tampa', 'Jacksonville', 'Naples'],
                'Illinois': ['Chicago', 'Springfield', 'Aurora', 'Naperville', 'Rockford']
            },
            'India': {
                'Maharashtra': ['Mumbai', 'Pune', 'Nagpur', 'Nashik', 'Aurangabad'],
                'Karnataka': ['Bangalore', 'Mysore', 'Hubli', 'Mangalore', 'Belgaum'],
                'Tamil Nadu': ['Chennai', 'Coimbatore', 'Madurai', 'Salem', 'Trichy'],
                'Delhi': ['New Delhi', 'Noida', 'Ghaziabad', 'Faridabad', 'Gurgaon'],
                'Gujarat': ['Ahmedabad', 'Surat', 'Vadodara', 'Rajkot', 'Gandhinagar']
            },
            'United Kingdom': {
                'England': ['London', 'Manchester', 'Birmingham', 'Liverpool', 'Leeds'],
                'Scotland': ['Edinburgh', 'Glasgow', 'Aberdeen', 'Dundee', 'Inverness'],
                'Wales': ['Cardiff', 'Swansea', 'Newport', 'Bangor', 'St Davids'],
                'Northern Ireland': ['Belfast', 'Derry', 'Lisburn', 'Bangor', 'Newry']
            },
            'Canada': {
                'Ontario': ['Toronto', 'Ottawa', 'Hamilton', 'London', 'Kingston'],
                'British Columbia': ['Vancouver', 'Victoria', 'Surrey', 'Burnaby', 'Richmond'],
                'Quebec': ['Montreal', 'Quebec City', 'Laval', 'Gatineau', 'Sherbrooke'],
                'Alberta': ['Calgary', 'Edmonton', 'Red Deer', 'Lethbridge', 'St. Albert']
            },
            'Australia': {
                'New South Wales': ['Sydney', 'Newcastle', 'Wollongong', 'Wagga Wagga', 'Albury'],
                'Victoria': ['Melbourne', 'Geelong', 'Ballarat', 'Bendigo', 'Shepparton'],
                'Queensland': ['Brisbane', 'Gold Coast', 'Cairns', 'Townsville', 'Toowoomba'],
                'Western Australia': ['Perth', 'Fremantle', 'Mandurah', 'Bunbury', 'Albany']
            }
        }

    def generate_location_data(self):
        """Generate matching country, state, and city"""
        country = random.choice(list(self.location_hierarchy.keys()))
        state = random.choice(list(self.location_hierarchy[country].keys()))
        city = random.choice(self.location_hierarchy[country][state])
        return country, state, city

    def generate_hierarchical_value(self, data_type, index, related_columns=None):
        """Generate values maintaining hierarchical relationships"""
        # Initialize location data if not in cache
        if index not in self.hierarchy_cache:
            country, state, city = self.generate_location_data()
            self.hierarchy_cache[index] = {
                "country": country,
                "state": state,
                "city": city
            }

        # Return appropriate value based on data type
        if data_type in ["country", "state", "city"]:
            return self.hierarchy_cache[index][data_type]
        
        # For non-hierarchical data types
        return self.generate_single_value(data_type)

    def generate_id(self, id_format='sequential', prefix=None, length=None):
        """
        Generate unique IDs based on different formats
        
        Args:
            id_format (str): Type of ID to generate ('sequential', 'uuid', 'hash')
            prefix (str): Optional prefix for the ID
            length (int): Optional length for hash-based IDs
        """
        if id_format == 'sequential':
            self.id_counter += 1
            id_value = str(self.id_counter).zfill(6)  # Pad with zeros
            return f"{prefix}{id_value}" if prefix else id_value

        elif id_format == 'uuid':
            return str(uuid.uuid4())

        elif id_format == 'hash':
            # Generate a random hash and take specified length
            random_string = f"{random.random()}{datetime.datetime.now()}"
            hash_object = hashlib.md5(random_string.encode())
            hash_id = hash_object.hexdigest()
            return hash_id[:length] if length else hash_id

        return None

    def generate_single_value(self, data_type, **kwargs):
        """Generate a single value based on data type"""
        if data_type == "id":
            id_format = kwargs.get('id_format', 'sequential')
            prefix = kwargs.get('prefix', '')
            length = kwargs.get('length', None)
            
            # Keep generating until we get a unique ID
            while True:
                new_id = self.generate_id(id_format, prefix, length)
                if new_id not in self.used_ids:
                    self.used_ids.add(new_id)
                    return new_id

        # Basic personal information
        elif data_type == "name":
            return self.fake.name()
        elif data_type == "email":
            return self.fake.email()
        elif data_type == "phone":
            return self.fake.phone_number()
        elif data_type == "address":
            return self.fake.address()
        elif data_type == "username":
            return self.fake.user_name()
        
        # Professional information
        elif data_type == "company":
            return self.fake.company()
        elif data_type == "job_title":
            return self.fake.job()

        # Internet and technology
        elif data_type == "url":
            return self.fake.url()
        elif data_type == "ip_address":
            return self.fake.ip_v4()

        # Financial information
        elif data_type == "credit_card":
            return self.fake.credit_card_number()

        # Date and numbers
        elif data_type == "date":
            return self.fake.date()
        elif data_type == "number":
            return self.fake.random_number(digits=kwargs.get('digits', 5))
        elif data_type == "boolean":
            return random.choice([True, False])
        elif data_type == "text":
            return self.fake.text(max_nb_chars=kwargs.get('max_chars', 200))

        # Categorized and range data
        elif data_type == "categorized":
            categories = kwargs.get('category_values', [])
            return random.choice(categories) if categories else None
        elif data_type == "range":
            start = kwargs.get('range_start', 0)
            end = kwargs.get('range_end', 100)
            return random.uniform(float(start), float(end))

        # Location data (handled by generate_hierarchical_value)
        elif data_type in ["country", "state", "city"]:
            return None  # This should not be called directly for these types

        return None

def generate_synthetic_data(columns_config, num_rows):
    """
    Generate synthetic data based on column configurations and number of rows
    """
    generator = HierarchicalDataGenerator()
    data = {}
    
    # First pass: Process ID columns first to ensure uniqueness
    id_columns = [col for col in columns_config if col['data_type'] == 'id']
    non_id_columns = [col for col in columns_config if col['data_type'] != 'id']
    
    # Sort columns to process IDs first, then hierarchical data, then regular data
    sorted_columns = id_columns + non_id_columns
    
    # Generate data for each column
    for column in sorted_columns:
        column_name = column['column_name']
        data_type = column['data_type']
        column_data = []
        
        for i in range(num_rows):
            if data_type == 'id':
                # Handle ID generation with custom format
                kwargs = {
                    'id_format': column.get('id_format', 'sequential'),
                    'prefix': column.get('prefix', ''),
                    'length': column.get('length', None)
                }
                value = generator.generate_single_value(data_type, **kwargs)
            elif data_type in ['country', 'state', 'city']:
                # Handle hierarchical data
                value = generator.generate_hierarchical_value(data_type, i)
            else:
                # Handle regular data
                kwargs = {}
                if data_type == 'categorized':
                    kwargs['category_values'] = column.get('category_values', [])
                elif data_type == 'range':
                    kwargs['range_start'] = column.get('range_start', 0)
                    kwargs['range_end'] = column.get('range_end', 100)
                
                value = generator.generate_single_value(data_type, **kwargs)
            
            column_data.append(value)
        
        data[column_name] = column_data
    
    return pd.DataFrame(data)

# Example usage
# if __name__ == "__main__":
#     # Example configuration with ID and hierarchical data
#     sample_config = [
#         {"column_name": "user_id", "data_type": "id", "id_format": "sequential", "prefix": "USER_"},
#         {"column_name": "transaction_id", "data_type": "id", "id_format": "uuid"},
#         {"column_name": "short_id", "data_type": "id", "id_format": "hash", "length": 8},
#         {"column_name": "full_name", "data_type": "name"},
#         {"column_name": "user_country", "data_type": "country"},
#         {"column_name": "user_state", "data_type": "state"},
#         {"column_name": "user_city", "data_type": "city"},
#         {"column_name": "status", "data_type": "categorized", "category_values": ["Active", "Inactive"]},
#     ]
    
#     # Generate 5 rows of synthetic data
#     df = generate_synthetic_data(sample_config, 5)
#     print("\nGenerated Synthetic Data:")
#     print(df)