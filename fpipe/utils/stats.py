# import sys
#
#
# class Stats:
#     def __init__(self, name: str, interval=2**12):
#         self.name = name
#         self.interval = interval
#         self.reads = 0
#         self.writes = 0
#
#         self.read_bytes = 0
#         self.written_bytes = 0
#
#     def p(self, length):
#         if (self.reads and self.reads % self.interval == 0) or self.writes and (
#                 self.writes % self.interval == 0) or length == 0:
#             print(f'{self.name}: R {self.reads}/{self.read_bytes}, W {self.writes}/{self.written_bytes}')
#             sys.stdout.flush()
#
#     def r(self, b: bytes):
#         self.reads += 1
#         length = len(b)
#         self.read_bytes += length
#         self.p(length)
#
#     def w(self, b: bytes):
#         self.writes += 1
#         length = len(b)
#         self.written_bytes += length
#         self.p(length)
#
