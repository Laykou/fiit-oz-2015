require 'active_record'

ActiveRecord::Base.establish_connection(adapter: 'postgresql', host: 'laykou.no-ip.org', port: 5432, user: 'postgres', 'password': 'postgres', database: 'oz')

sql = "SELECT id,score, text FROM reviews ORDER BY RANDOM() LIMIT 2000" ##
records_array = ActiveRecord::Base.connection.execute(sql)

dir_paths = {}
for i in 0..5
	dir_paths["#{i}.0"] = "./reviews/#{i}.0"
end

records_array.each do |rec| 
		f = File.new "#{dir_paths[rec["score"]]}/#{rec["id"]}.txt", 'w'
		f.write(rec["text"])
		f.close
end 
