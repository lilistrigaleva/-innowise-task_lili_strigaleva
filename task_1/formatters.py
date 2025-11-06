import json
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod

class Formatter(ABC):
    """Абстрактный базовый класс для форматеров."""
    @abstractmethod
    def format(self, data: dict) -> str:
        pass

class JSONFormatter(Formatter):
    """Форматирует данные в JSON."""
    def format(self, data: dict) -> str:
        return json.dumps(data, indent=4, default=str) 

class XMLFormatter(Formatter):
    def format(self, data: dict) -> str:
        root = ET.Element("results")
        for query_name, records in data.items():
            query_element = ET.SubElement(root, query_name)
            for record in records:
                record_element = ET.SubElement(query_element, "record")
                for key, val in record.items():
                    field = ET.SubElement(record_element, key)
                    field.text = str(val)
        
        ET.indent(root, space="\t", level=0)
        return ET.tostring(root, encoding='unicode')

def get_formatter(format_type: str) -> Formatter:
    if format_type.lower() == 'json':
        return JSONFormatter()
    elif format_type.lower() == 'xml':
        return XMLFormatter()
    else:
        raise ValueError(f"Неподдерживаемый формат: {format_type}")