from pyedi import X12Parser, StructuredFormatter

class EDIProcessor:
    def __init__(self):
        self.parser = X12Parser()
        self.formatter = StructuredFormatter()

    def parse(self, file_path: str):
        # 1. Read the raw EDI file
        with open(file_path, "r", encoding="utf-8") as file:
            edi_data = file.read()

        # 2. Convert raw EDI text to generic JSON structure
        generic_json = self.parser.parse(edi_data)

        # 3. Format it immediately into the structured format your pipeline expects
        structured_json = self.formatter.format(
            generic_json,
            include_technical=True
        )

        return structured_json
