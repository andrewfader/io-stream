# frozen_string_literal: true

# Released under the MIT License.
# Copyright, 2026, by Andrew Fader.

require "io/stream/buffered"
require "objspace"

# Deliver exact read sizes while honoring sysread's output-buffer contract.
# Socket writes cannot guarantee these boundaries, and this source never blocks.
class ScriptedReadableStream < IO::Stream::Generic
	def initialize(chunks)
		@chunks = chunks.dup
		@destinations = []
		super()
	end
	
	attr :destinations
	
	protected
	
	def sysread(size, buffer)
		return unless chunk = @chunks.shift
		raise "Scripted chunk exceeds requested read size" if chunk.bytesize > size
		
		@destinations << buffer
		buffer.clear
		buffer << chunk
		return buffer
	end
	
	def sysread_nonblock(size, buffer)
		sysread(size, buffer)
	end
end

describe IO::Stream::Readable do
	# The smallest read_partial that is handed over without a copy:
	threshold = IO::Stream::MINIMUM_READ_SIZE / IO::Stream::HANDOVER_RATIO
	
	with "read buffer ownership" do
		[threshold - 1, threshold, threshold + 1].each do |size|
			with "a #{size}-byte read", unique: size do
				it "copies only below the threshold and preserves returned data across later reads" do
					first = "a".b * size
					second = "b".b * threshold
					third = "c".b * 64
					stream = ScriptedReadableStream.new([first, second, third])
					
					chunk = stream.read_partial
					expect(chunk).to be == first
					expect(chunk.encoding).to be == Encoding::BINARY
					if size < threshold
						expect(chunk).not.to be_equal(stream.destinations.first)
					else
						expect(chunk).to be_equal(stream.destinations.first)
					end
					
					next_chunk = stream.read_partial
					expect(next_chunk).to be == second
					expect(chunk).to be == first
					expect(stream.read_partial).to be == third
					expect(chunk).to be == first
					expect(next_chunk).to be == second
					expect(stream.read_partial).to be_nil
				end
			end
		end
		
		it "reuses the input buffer for small reads without changing retained chunks" do
			chunks = ["a".b * 64, "b".b * 64, "c".b * 64]
			stream = ScriptedReadableStream.new(chunks)
			retained = chunks.map{stream.read_partial}
			
			expect(retained).to be == chunks
			expect(stream.destinations[1]).to be_equal(stream.destinations[0])
			expect(stream.destinations[2]).to be_equal(stream.destinations[0])
			expect(stream.read_partial).to be_nil
		end
		
		it "appends a large read to buffered data without losing either part" do
			first = "a".b * 64
			second = "b".b * threshold
			stream = ScriptedReadableStream.new([first, second, "end".b])
			
			chunk = stream.read(first.bytesize + second.bytesize)
			expect(chunk).to be == first + second
			expect(stream.read_partial).to be == "end"
			expect(chunk).to be == first + second
			expect(stream.read_partial).to be_nil
		end
		
		it "preserves slices of a handed-over buffer across subsequent reads" do
			first = "a".b * threshold
			second = "b".b * threshold
			stream = ScriptedReadableStream.new([first, second])
			
			prefix = stream.read_partial(64)
			remainder = stream.read_partial
			expect(prefix).to be == first.byteslice(0, 64)
			expect(remainder).to be == first.byteslice(64..)
			expect(stream.read_partial).to be == second
			expect(prefix + remainder).to be == first
			expect(stream.read_partial).to be_nil
		end
		
		it "copies a small read into a string of exactly its size" do
			# Just over a power of two, where appending to an empty string would almost double the capacity:
			chunk = "a".b * (threshold / 2 + 1)
			stream = ScriptedReadableStream.new([chunk])
			
			copy = stream.read_partial
			expect(copy).to be == chunk
			expect(copy.encoding).to be == Encoding::BINARY
			expect(ObjectSpace.memsize_of(copy)).to be < chunk.bytesize + 1024
		end
		
		it "compares against the largest earlier read, not the current one" do
			large = IO::Stream::MINIMUM_READ_SIZE * 4
			stream = ScriptedReadableStream.new(["a".b * 64, "b".b * 64, "c".b * threshold])
			expect(stream.read_partial(32)).to be == "a".b * 32
			
			# A large read, as `read(large)` would make with data already buffered, grows the reused input buffer but returns only a small chunk:
			stream.send(:fill_read_buffer, large)
			expect(stream.read_partial).to be == "a".b * 32 + "b".b * 64
			
			# The input buffer is now much larger than this read, so it is copied too:
			chunk = stream.read_partial
			expect(chunk).to be == "c".b * threshold
			expect(chunk).not.to be_equal(stream.destinations.last)
		end
		
		it "reads a large read straight into the buffer it returns" do
			size = IO::Stream::MINIMUM_READ_SIZE * 4
			part = "a".b * (size / 16)
			stream = ScriptedReadableStream.new([part] * 16)
			
			chunk = stream.read(size)
			expect(chunk).to be == part * 16
			
			# The first read is far below the handover threshold, but is not copied out, so the buffer is allocated once at the size asked for. Later reads are appended from the input buffer:
			expect(stream.destinations.size).to be == 16
			expect(chunk).to be_equal(stream.destinations.first)
			stream.destinations.drop(1).each do |destination|
				expect(destination).not.to be_equal(chunk)
				expect(destination).to be_equal(stream.destinations[1])
			end
		end
	end
	
	with "#peek_partial" do
		[threshold - 1, threshold].each do |size|
			with "a #{size}-byte read", unique: size do
				it "copies only below the threshold" do
					data = "a".b * size
					stream = ScriptedReadableStream.new([data, "end".b])
					
					expect(stream.peek_partial).to be == data
					chunk = stream.read_partial
					expect(chunk).to be == data
					if size < threshold
						expect(chunk).not.to be_equal(stream.destinations.first)
					else
						expect(chunk).to be_equal(stream.destinations.first)
					end
					
					expect(stream.read_partial).to be == "end"
					expect(chunk).to be == data
				end
			end
		end
		
		it "sizes the chunk a probe reads to the data read" do
			input, output = IO.pipe
			stream = IO::Stream::Buffered.wrap(input)
			output.write("Hello World!")
			output.close
			
			expect(stream.peek_partial).to be == "Hello World!"
			chunk = stream.read_partial
			expect(chunk).to be == "Hello World!"
			expect(ObjectSpace.memsize_of(chunk)).to be < 1024
		ensure
			stream&.close
			output&.close
		end
	end
end
