require 'elasticsearch'
require 'ruby-progressbar'
require 'sanitize'

BULK_SIZE = 20000

@client = Elasticsearch::Client.new host: 'localhost:9200'
lines = File.open("movies.txt").each_line
@bulk = []

def parse_review_line(line)
	return [nil, nil] if line.empty?

	e = line.chars.select(&:valid_encoding?).join.split(':', 2)

	e.empty?

	!e.nil? ? e.strip : ''
end

def sanitize(key, text)
	text ||= ''

	return text.split('/').collect(&:strip).collect(&:to_f) if key == 'helpfulness'
	return text.to_f if key == 'score'
	text = Sanitize.clean(text) if key == 'text' ||  key == 'summary'

	text.strip
end

def put_review(rev)
	@bulk << rev

	if(@bulk.length >= BULK_SIZE)
		@client.bulk body: @bulk
		@bulk = []
	end
end

progress_bar = ProgressBar.create(title:'reviews', total: 7911684,throttle_rate: 1, format: '%a | %e %B %p%% %t')

review = {}

count = 0
lines.each do |line|
	begin
		next if line.empty?

		line = line.chars.select(&:valid_encoding?).join
		e = line.split(':', 2)

		next if e.empty?

		if e[1].nil?
			review[review.keys.last] += ' ' + sanitize(review.keys.last, e[0])
			next
		end

		key = e[0].split('/')[1]

		if !["productId", "userId", "profileName", "helpfulness", "score", "time", "summary", "text"].include?(key)
			review[review.keys.last] += ' ' + sanitize(review.keys.last, line)
			next
		end

		if key == 'productId' && !review.empty?
			# puts "----"
			# puts review.inspect

			req = { index: {_index: 'reviews', _type: 'review', data: review} }

			put_review req     
			progress_bar.increment

			count += 1

			review = {}
		end

		review[key] = sanitize(key, e[1])

		#	rescue ArgumentError
		#		puts "Skipped " + lines.inspect
	end
end

if(@bulk.length > 0)
	@client.bulk body: @bulk
	@bulk = []
end

puts count
