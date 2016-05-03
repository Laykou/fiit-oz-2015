require 'active_record'

ActiveRecord::Base.establish_connection(adapter: 'postgresql', host: 'localhost', port: 5432, user: 'postgres', 'password': 'postgres', database: 'oz')

# sql = "SELECT id,score, text FROM reviews ORDER BY RANDOM() LIMIT 2000" ##
# records_array = ActiveRecord::Base.connection.execute(sql)

dir_paths = {}
for i in 0..5
	dir_paths["#{i}.0"] = "/media/laykou/Video/oz/reviews/#{i}.0"
end

# records_array.each do |rec| 
#		f = File.new "#{dir_paths[rec["score"]]}/#{rec["id"]}.txt", 'w'
#		f.write(rec["text"])
#		f.close
# end 

LIMIT = 1000
page = 0

while true do
    sql = "SELECT id,score, text FROM reviews ORDER BY id LIMIT #{LIMIT} OFFSET #{LIMIT * page}"
    records_array = ActiveRecord::Base.connection.execute(sql)

    if records_array.num_tuples.zero?
        break;
    end

    records_array.each do |rec| 
        f = File.new "#{dir_paths[rec["score"]]}/#{rec["id"]}.txt", 'w'
        f.write(rec["text"])
        f.close
    end

    page += 1
end
