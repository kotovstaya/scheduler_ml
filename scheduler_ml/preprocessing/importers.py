class BaseImporter:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def run(self, input_file, output_file):
        df = self.reader.read(input_file)
        self.writer.write(df, output_file)
