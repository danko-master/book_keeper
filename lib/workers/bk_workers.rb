require 'json'
require 'bunny'

module BkWorkers
  class Credit
    include Sidekiq::Worker
    sidekiq_options queue: :bk_credit

    def perform(num_inst)
      @experiment_logger = []

      @bunny = Bunny.new(host: $config['rabbit']['host'], 
        port: $config['rabbit']['port'], 
        user: $config['rabbit']['user'], 
        password: $config['rabbit']['password'])
      @current_logger = Logger.new("#{File.dirname(__FILE__)}/../../log/sidekiq_#{ENV['APP_ENV']}_credit_inst_#{num_inst}.log")
      @current_logger.info "NOTIFICATIONS: Credit Started"      
      begin
        @current_logger.info p " [*] RUBY Waiting for messages. To exit press CTRL+C"
        @bunny.start
        @ch   = @bunny.create_channel
        run
      rescue Interrupt => _
        @bunny.close
        @current_logger.info "NOTIFICATIONS: Credit Stopped"
        exit(0)
      end
    end

    def run     
      @current_logger.info p "Выполняем run, ждем tdr. input_queue_credit #{$config['runner']['input_queue_credit']}"  
      q    = @ch.queue($config['runner']['input_queue_credit'], :durable => true) 
      q.subscribe(:block => true, :manual_ack => true) do |delivery_info, properties, body|
        begin
          tdr_data = Hash.new
          tdr_data['delivery_tag'] = delivery_info.delivery_tag
          tdr_data['tdr'] = body 
          @current_logger.info p "Bunny ::: получили данные #{tdr_data}"


          if tdr_data.present?
            tdr = JSON.parse(tdr_data['tdr'])
            @current_logger.info p "Новый tdr из #{tdr}"
            p credit_rabbit = tdr['sum'].to_f

            obd = RedisHelper.on_board_device(tdr['imei'])
            p "obd #{obd}"

            obd_truck = RedisHelper.truck(obd['truck_id']) if obd.present?
            p "obd_truck #{obd_truck}"
  
            if obd_truck.present?
              company_account_id = obd_truck['company_account_id']
              p company_account = RedisHelper.company_account(company_account_id) 
            end
            p "company_account #{company_account}"

            if company_account.present?
              p credit = company_account['credit'].to_f
              p debit = company_account['debit'].to_f

              credit = credit + credit_rabbit
              @current_logger.info p "debit - credit #{debit - credit}"
              if debit - credit < 100
                @current_logger.info p "#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']} превышен порог"
                @current_logger.info p "#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']} ставим флаг 1" 
                $redis_alarm.set("#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']}", 1)
                flag = 1
              else
                @current_logger.info p "#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']} все нормально"
                @current_logger.info p "#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']} ставим флаг 0"
                $redis_alarm.set("#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']}", 0)
                flag = 0
              end

              company_values = company_account
              company_values['credit'] = credit
              RedisHelper.update_company_account(company_account_id, company_values, flag)

              send_tdr_data_to_rabbit(tdr, credit, company_account_id, obd['id'])
            end

            delivery_tag = tdr_data['delivery_tag']
            # # отправка ack в канал
            @current_logger.info p "Отправка ack в RabbitMQ ::: delivery_tag: #{delivery_tag}"
            @ch.ack(delivery_tag)

            @current_logger.info p "Обработан tdr #{tdr}"  
          else
            # Заглушка 
            @current_logger.info p "Принудительная Отправка ack в RabbitMQ ::: delivery_tag: #{delivery_info.delivery_tag}"
            @ch.ack(delivery_info.delivery_tag)
          end

        rescue Exception => e
          puts "ERROR! #{e}"
        end
      end
    end

    def send_tdr_data_to_rabbit(tdr, sum, company_account_id, on_board_device_id)
      @current_logger.info p "Отправка tdr в RabbitMQ #{tdr} ::: sum: #{sum} ::: company_account_id #{company_account_id}"
      q    = @ch.queue($config['runner']['output_queue'], :durable => true)

      h = {
        on_board_device_id: on_board_device_id,
        sum: tdr['sum'],
        company_account_id: company_account_id,
        kilometers: tdr['path'],
        payment_type: "write_off"
      }
      tdr_doc = h.to_json

      @ch.default_exchange.publish(tdr_doc, :routing_key => q.name)
    end
  end

  class Debit
    include Sidekiq::Worker
    sidekiq_options queue: :bk_debit

    def perform(num_inst)
      @experiment_logger = []

      @bunny = Bunny.new(host: $config['rabbit']['host'], 
        port: $config['rabbit']['port'], 
        user: $config['rabbit']['user'], 
        password: $config['rabbit']['password'])
      @current_logger = Logger.new("#{File.dirname(__FILE__)}/../../log/sidekiq_#{ENV['APP_ENV']}_debit_inst_#{num_inst}.log")
      @current_logger.info "NOTIFICATIONS: Debit Started"      
      begin
        @current_logger.info p " [*] RUBY Waiting for messages. To exit press CTRL+C"
        @bunny.start
        @ch   = @bunny.create_channel
        run
      rescue Interrupt => _
        @bunny.close
        @current_logger.info "NOTIFICATIONS: Debit Stopped"
        exit(0)
      end
    end

    def run 
      @current_logger.info p "Выполняем run, ждем tdr. input_queue_debit #{$config['runner']['input_queue_debit']}"  
      q    = @ch.queue($config['runner']['input_queue_debit'], :durable => true) 
      q.subscribe(:block => true, :manual_ack => true) do |delivery_info, properties, body|
      begin
        tdr_data = Hash.new
        tdr_data['delivery_tag'] = delivery_info.delivery_tag
        tdr_data['tdr'] = body 
        @current_logger.info p "Bunny ::: получили данные #{tdr_data}"


        if tdr_data.present?
          tdr = JSON.parse(tdr_data['tdr'])
          @current_logger.info p "Новый payment debit из #{tdr}"
          p debit_rabbit = tdr['sum'].to_f

          # obd = RedisHelper.on_board_device(tdr['imei'])
          # p "obd #{obd}"

          # obd_truck = RedisHelper.truck(obd['truck_id']) if obd.present?
          # p "obd_truck #{obd_truck}"

          # if obd_truck.present?
          #   company_account_id = obd_truck['company_account_id']
          #   p company_account = RedisHelper.company_account(company_account_id) 
          # end
          p company_account = RedisHelper.company_account(tdr['company_account_id'])
          p "company_account #{company_account}"

          if company_account.present?
            p debit = company_account['debit'].to_f
            p credit = company_account['credit'].to_f
            debit = debit + debit_rabbit

            company_account_id = company_account['id']

            @current_logger.info p "debit - credit #{debit - credit}"
            if debit - credit < 100
              @current_logger.info p "#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']} превышен порог"
              @current_logger.info p "#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']} ставим флаг 1" 
              @current_logger.info p "#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']}"
              $redis_alarm.set("#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']}", 1)
              flag = 1
            else
              @current_logger.info p "#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']} все нормально"
              @current_logger.info p "#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']} ставим флаг 0"
              @current_logger.info p "#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']}"
              $redis_alarm.set("#{$config['redis_alarm']['prefix']}:#{tdr['company_account_id']}", 0)
              flag = 0
            end

            company_values = company_account
            company_values['debit'] = debit
            RedisHelper.update_company_account(company_account_id, company_values, flag)

            send_tdr_data_to_rabbit(tdr, debit, company_account_id)
          end

          delivery_tag = tdr_data['delivery_tag']
          # # отправка ack в канал
          @current_logger.info p "Отправка ack в RabbitMQ ::: delivery_tag: #{delivery_tag}"
          @ch.ack(delivery_tag)

          @current_logger.info p "Обработан tdr #{tdr}"  
        else
          # Заглушка 
          @current_logger.info p "Принудительная Отправка ack в RabbitMQ ::: delivery_tag: #{delivery_info.delivery_tag}"
          @ch.ack(delivery_info.delivery_tag)
        end

      rescue Exception => e
        puts "ERROR! #{e}"
      end
      end
    end

    def send_tdr_data_to_rabbit(tdr, sum, company_account_id)
      @current_logger.info p "Отправка tdr в RabbitMQ #{tdr} ::: sum: #{sum} ::: company_account_id #{company_account_id}"
      q    = @ch.queue($config['runner']['output_queue'], :durable => true)

      h = {
        on_board_device_id: "null", 
        sum: tdr['sum'],
        company_account_id: company_account_id,
        kilometers: 0,
        payment_type: "refill"
      }
      tdr_doc = h.to_json

      @ch.default_exchange.publish(tdr_doc, :routing_key => q.name)
    end
  end

  class RedisHelper
    def self.on_board_device(imei)
      result = $redis.get("#{$config['redis_cache']['prefix']}:on_board_device:#{imei}")
      result = eval( result ) if result.present?
      result
    end

    def self.truck(truck_id)
      result = $redis.get("#{$config['redis_cache']['prefix']}:truck:#{truck_id}")
      result = eval( result ) if result.present?
      result
    end

    def self.company_account(company_account_id)
      result = $redis.get("#{$config['redis_cache']['prefix']}:company_account:#{company_account_id}")
      result = eval( result ) if result.present?
      result
    end

    def self.update_company_account(company_account_id, company_values, flag)
      p "update_company_account"
      p "company_account_id #{company_account_id}"
      p "company_values #{company_values}"
      $redis.set("#{$config['redis_cache']['prefix']}:company_account:#{company_account_id}", company_values)
      

      # костыль для PG пока не будет готово обработчик
      # обновляем CompanyAccount
      p ca = Db::CompanyAccount.find_by_id(company_account_id)
      p ca.debit = company_values['debit']
      p ca.credit = company_values['credit']
      p "ca flag #{flag}"
      p ca.balance = flag
      ca.save
    end
  end
end