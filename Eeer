class InvestMap

   require 'find'
   require 'fileutils'
   require 'spreadsheet'
   require 'net/http'

   include ModelModules::FilesService
   include ModelModules::FtpService


   ROOT_PATH =
       if Rails.env == 'production'
          '/tmp'
       else
          Rails.root.join('db')
       end

   @rus = 0
   @eng = 0
   AREA_TYPES = {
      "Модуль с прилегающими бытовыми помещениями": "Module with adjacent domestic premises",
      "Земельные участки": "Land plot",
      "Территория незавершенного строительства": "Construction in progress area",
      "Складское помещение": "Warehouse",
      "Производственная база (перечень оборудования)": "Production base (list of equipment)",
      "Здание предприятия (наименование)": "The building of the enterprise (name)",
      "Помещение": "Room",
      "Бесхоз": "Ownerless",
      "Иное": "Otherwise"
   }
   AREA_CONDITIONS = {
      "Требует вложения средств": "Requires investment",
      "Необходим ремонт": "Repair needed",
      "Продан по торгам": "Sold by auction",
      "Реализован субъектом МСП": "Implemented by MSP",
      "Передан в аренду": "Leased out",
      "Передан в аренду МСП": "Leased to MSP"
   }

   OFFERS_FOR_USING = {
      "Сельское хозяйство": "agriculture",
      "Животноводство": "stock_raising",
      "Промышленное производство": "industry",
      "Транспорт и хранение": "transport_and_storage",
      "Индустриальные парки": "industrial_parks",
      "Технопарки": "technoparks",
      "Отдых и туризм": "recreation_and_tourism",
      "Оптовая и розничная торговля": "wholesale_and_retail_trading",
      "Питание": "nutrition",
      "Образование": "education",
      "Здравоохранение": "health_care",
      "Общественно-деловое значение": "social-business_purpose",
      "Инженерные коммуникации": "engineering_communications",
      "Индустриальный парк типа greenfield": "greenfield_type_industrial_park",
      "Индустриальный парк типа brownfield":"brownfield_type_industrial_park",
      "Иное": "other"
   }
   DISTRICTS_NAMES = {
      "Абзелиловский": "Abzelilovski district",
      "Альшеевский": "Alsheevsky district",
      "Архангельский": "Arkhangelsk district",
      "Аскинский": "Askinskiy district",
      "Аургазинский": "Aurgazinsky district",
      "Баймакский": "Baymak district",
      "Бакалинский": "Bakalinsky district",
      "Балтачевский": "Baltachevsky district",
      "Белебеевский": "Belebeyevsky district",
      "Белокатайский": "Belokatay district",
      "Белорецкий": "Beloretsky district",
      "Бижбулякский": "Bizhbulyaksky district",
      "Бирский": "Birsky district",
      "Благоварский": "Blagovarsky district",
      "Благовещенский": "Blagoveshchensk district",
      "Буздякский": "Buzdyak district",
      "Бураевский": "Buraevsky district",
      "Бурзянский": "Burzyansky district",
      "Гафуриский": "Gafuri district",
      "Давлекановский": "Davlekanovskiy district",
      "Дуванский": "Duvan district",
      "Дюртюлинский": "Dyurtyulinsky district",
      "Ермекеевский": "Ermekeevskogo district",
      "Зиянчуринский": "Zianchurinsky district",
      "Зилаирский": "Zilair district",
      "Иглинский": "Iglinsky district",
      "Илишевский": "Ilishevsky district",
      "Ишимбайский": "Ishimbay district",
      "Калтасинский": "Kaltasinsky district",
      "Караидельский": "Karaidel'skiy district",
      "Кармаскалинский": "Karmaskalinsky district",
      "Кигинский": "Kiginsky district",
      "Краснокамский": "Krasnokamsky district",
      "Кугарчинский": "Kugarchinsky district",
      "Куюргазинский": "Kuyurgazinsky district",
      "Кушнаренковский": "Kushnarenko district",
      "Мелеузовский": "Meleuzovsky district",
      "Метелинский": "Mechetlinsky district",
      "Мишкинский": "Mishkin district",
      "Миякинский": "Miyakinsky district",
      "Нуримановский": "Nurimanovskiy district",
      "Салаватский": "Salavat district",
      "Стерлибашевский": "Sterlibashevsky district",
      "Стерлитамакский": "Sterlitamak district",
      "Татышлинский": "Tatyshlinsky district",
      "Туймазинский": "Tuimazinsky district",
      "Уфимский": "Ufimski district",
      "Учалинский": "Uchalinsky district",
      "Федоровский":"Fedorovsky district",
      "Хайбуллинский":"Haybullinsky district",
      "Чекмагушевский": "Chekmagushevsky district",
      "Чишминский": "Chishminsky district",
      "Шаранский": "Sharansky district",
      "Янаульский": "Yanaul'skiy district",
      "Кумертау": "City district Kumertau",
      "Нефтекамск": "Neftekamsk city district",
      "Уфа": "Ufa city district",
      "Агидель": "Agidel city district",
      "Октябрьский": "Oktyabrsky city district",
      "Салават": "Salavat city district",
      "Сибай": "Sibai city district",
      "Стерлитамак": "Sterlitamak city district",
      "Межгорье": "Urban district BUT Mezhgorye"
   }


   # @const [Pathname] - путь файла логов.
   LOG_FILE = Rails.root.join('log', 'invest_map.log')

   XLS_PATH = "#{ROOT_PATH}/imports/inventory"

   # папка для хранения сводных таблиц при локальном тестировании импорта.
   LOCAL_IMPORT_FILES = "#{ROOT_PATH}/imports/inventory-local"

   FTP_DOMAIN_NAME = '10.'
   FTP_PORT = 21
   FTP_LOGIN = 'sokol'
   FTP_PASSWORD = 'D'

   REGION_GUID = "6f2cbfd8-692a-4"

   ROOT_FTP_FOLDER = "INVEST_MAP"
   POWER_FTP_FOLDER = 'Мощности'
   INVEST_FTP_FOLDER = 'БАШКОРТОСТАН (РЕСП)_ОСН'
   FIELD_FTP_FOLDER = 'Месторождения'
   GAS_FTP_FOLDER = 'Газораспределение'


   TARGET_FTP_FOLDER = "phoenix"
   ROUTE_FILE_NAME = 'routes.xlsx'
   REMOTE_ROUTE_FILE_PATH = "/#{ROOT_FTP_FOLDER}/SERVICE/routes.xlsx"
   PATH_OPTIONS = Hash[
       :full_folder_path, 'full_folder_path',
       :action, 'action'
   ]

   EXCLUDE_SUFFIX = ['_COMPLETE', '_ERRORS', '_service']

   CONTACTS_INFO = 'Контакты'
   CAPACITY = 'Мощности'
   OBJECTS_INFO = 'Объекты (ЗУ и ОКС)'
   FIELD_INFO = 'Месторождения'

   AREA_REF = 'Районы (города)'
   LOCALITY_REF = 'Населенные пункты'
   VILLAGE_COUNCIL_REF = 'Сельсоветы'
   TYPE_FIELD_REF = 'Типы месторождений'
   TYPE_UNIT_REF = 'Единицы измерения'

   OBJECTS_AREA_TYPE ='Объекты'
   OBJECTS_MINIMAL_FILE_VERSION = '1.4.6'

   POWER_AREA_TYPE ='Энергоучёт'
   POWER_MINIMAL_FILE_VERSION = '1.4.6'

   GAS_AREA_TYPE ='Газораспределение'
   POWER_MINIMAL_FILE_VERSION = '1.4.1'


   FIELD_AREA_TYPE ='Месторождения'
   FIELD_MINIMAL_FILE_VERSION = '1.1.1'

   ERROR_SHEET_NAME = 'Ошибки'

   NO_DATA_RUS = 'не предоставлено поставщиком данных'
   NO_DATA_ENG = 'not provided by the data provider'

   @counts =  {
       :files => 0,
       :objects => 0
   }

   @errors = 0
   @error_list = []

   @logger = Logger.new(LOG_FILE)

   @file_stat = {
      :count => 0,
      :success => 0,
      :error => 0,
      :excluded => 0,
      :file_error => 0
   }
   @pass_stat


   # @const [Hash] - тексты заголовков записей лога.
   LOGGER_TEXTS = {
       start: 'Начат процесс обработки файлов',
       start_process_read_catalog_structure: 'Начат процесс проверки/создания структуры каталогов',
       work_ftp: 'Работа по FTP',
       testing: 'Режим тестирования',
       missed: 'пропущен',
       work_started: 'обработка начата',
       work_time: 'Обработка заняла ',
       end: 'Процесс обработки файлов завершен',
       footer: '_______________________________________________________',
       limit: 'Заданное кол-во файлов',
       start_file_index: 'Начальный индекс файла'
   }
   # @const [String] - разделитель заголовка и значения записи лога.
   LOG_SEPARATOR = ': '


   # Метод проверяет структуру каталогов на FTP по файлу /INVEST_MAP/SERVICE/routes.xlsx,
   # при остутствии каталогов - создает их
   #
   # @return [nil]
   #
   def self.read_catalog_structure_file
      logger_texts = LOGGER_TEXTS
      xls_path = XLS_PATH
      route_file_name = ROUTE_FILE_NAME
      remote_routes_file_path = REMOTE_ROUTE_FILE_PATH
      root_ftp_folder = ROOT_FTP_FOLDER
      path_options = PATH_OPTIONS
      stoped = false


      @logger.info(logger_texts[:footer])
      @logger.info(logger_texts[:start_process_read_catalog_structure])

      @ftp = ftp_connect
      begin
         @ftp.getbinaryfile(remote_routes_file_path, [xls_path , route_file_name].join('/'))
      rescue => error
         @logger.info(["Ошибка копирования файла с FTP:", remote_routes_file_path].join(" "))
         stoped = true
      end

      unless stoped
         file_path = [xls_path , route_file_name].join('/')
         book = Roo::Spreadsheet.open(file_path)

         row_count = 0
         counts = {
             :create => 0,
             :delete_catalog => 0,
             :delete_files => 0
         }

         book.each_with_pagename do |page_name, page_data|

            path_column_num = page_data.row(page_data.header_line)
                                  .index(path_options[:full_folder_path])
            path_column_num += 1

            action_column_num = page_data.row(page_data.header_line)
                                    .index(path_options[:action])
            action_column_num += 1

            paths = page_data.column(path_column_num)
            binding_progressbar = get_progressbar({total:paths.count, title:"Обработка файла со структурой каталогов"})
            paths.each do |path|
               row_count += 1
               binding_progressbar.format = "%t |%B| [%f] %p%%  Обработано %c/%C записей, создано: #{counts[:create]}, удалено: каталогов: #{counts[:delete_catalog]}, файлов: #{counts[:delete_files]}"

               if row_count > 1
                  option = page_data.row(row_count)[action_column_num] if action_column_num.present?
                  result = check_ftp_path("/#{root_ftp_folder}/#{path}", option)

                  counts[:create] += result[:create]
                  counts[:delete_catalog] += result[:delete_catalog]
                  counts[:delete_files] += result[:delete_files]

               end
               binding_progressbar.increment
            end

         end

         @logger.info(["Каталогов создано:", counts[:create],
                       ", удалено: каталогов:", counts[:delete_catalog],
                       ", файлов:", counts[:delete_files]].join(" "))

         Dir.glob(xls_path + '/*.xlsx').each { |file| File.delete(file)}
      end
      nil
   end


   # Вариант работы обходчика: Повседневный
   #
   # @return nil
   #
   def self.import_every_day
      import
      import(is_power: true)
      read_catalog_structure_file
   end


   # Вариант работы обходчика: Перезагрузка
   #
   # @param from_ftp[Boolean] - флаг, запускать проверку по ftp (true) или локально (false)
   #
   # @return nil
   #
   def self.import_with_reload(from_ftp: nil)
      reset_import

      rename_invest_files(from_ftp, is_power_only: false)
      import(is_gas: true)
      import(is_field: true)
      import(is_power: true)
      import(from_ftp: from_ftp)
      read_catalog_structure_file

      backup_data
   end


   # Вариант работы обходчика: Мощности
   #
   # @return nil
   #
   def self.import_power_only
      reset_power_statistic
      rename_invest_files(nil, is_power_only: true)
      import(is_power: true)
   end


   # Переименовывает файлы с префиксами _COMPLETE и _ERROR в папке инвест объектов
   #
   # @param from_ftp[Boolean] - флаг, запускать проверку по ftp (true) или локально (false)
   # @param is_power_only[Boolean] - флаг, проверять ли только объекты-мощности (true)
   #
   # @return nil
   #
   def self.rename_invest_files(from_ftp, is_power_only: false)

      if from_ftp == nil
         from_ftp = detect_from_ftp(ROOT_FTP_FOLDER)
      end

      if from_ftp
         rename_files_on_ftp(root_folder: ROOT_FTP_FOLDER, main_folder: POWER_FTP_FOLDER)

         unless is_power_only
            rename_files_on_ftp(root_folder: ROOT_FTP_FOLDER, main_folder: INVEST_FTP_FOLDER)
         end
      else

         start_path_power = "#{ROOT_FTP_FOLDER}/#{POWER_FTP_FOLDER}" #FTP должен быть примонтирован к /mnt/ftp
         start_path_invest = "#{ROOT_FTP_FOLDER}/#{INVEST_FTP_FOLDER}"
         start_path_field = "#{ROOT_FTP_FOLDER}/#{FIELD_FTP_FOLDER}"
         start_path_gas = "#{ROOT_FTP_FOLDER}/#{GAS_FTP_FOLDER}"

         rename_files_on_local(root_folder: start_path_power)


         unless is_power_only
         rename_files_on_local(root_folder: start_path_gas)

         rename_files_on_local(root_folder: start_path_invest)

         rename_files_on_local(root_folder: start_path_field)

         end
      end
   end



   # Основная процедура запуска импорта из xls файлов устанавливает соединение
   #  с FTP сервером и запускает поиск и обработку файлов.
   #
   # При testing == true не изменяет файлы на сервере, но собирает обработанные файлы
   #   в папке проекта db/imports/inventory-test, при тестировании на сервере
   # файлы собираются в папку /tmp/imports/inventory-test
   #
   # При локальном тестировании обрабатывается папка db/imports/inventory-local
   #
   # example InvestMap.import(true, testing:true) - тестирование FTP
   #         InvestMap.import(true) - рабочий боевой режим
   #
   # @param from_ftp[Boolean] - режим работы с FTP (по-умолчанию -true).
   # @param limit[Integer] - максимальное кол-во файлов, которые будут обработаны,
   #                             используется только для тестирования FTP (по-умолчанию -nil).
   # @param start_index[Integer] - индекс начального файла, для попадания в обработку (по-умолчанию -nil).
   # @param only_count[Boolean] - признак подсчитать только количество файлов.
   # @param is_power[Boolean] - флаг, импортировать ли только объекты-мощности (true)
   # @param testing[Boolean] - флаг, запускать ли обход и проверку данных в тестовом режиме (true)
   # @param is_power[Boolean] - флаг, импортировать ли только объекты-месторождения (true)
   #
   # @return nil
   #
   def self.import(from_ftp: nil, start_index: nil, limit: nil, only_count: nil,is_gas: false, is_power: false, testing: false, is_field: false)

      logger_texts = LOGGER_TEXTS
      separator = LOG_SEPARATOR
      @logger.info(logger_texts[:footer])
      @logger.info(logger_texts[:start])

      if from_ftp == nil

         if File.directory?("/mnt/ftp/#{ROOT_FTP_FOLDER}") #FTP должен быть примонтирован к /mnt/ftp
            from_ftp = false
         else
            from_ftp = true
         end
      end

      @logger.info([logger_texts[:work_ftp], from_ftp].join(separator))
      @logger.info([logger_texts[:limit], limit].join(separator)) if limit.present?
      @logger.info([logger_texts[:start_file_index], start_index].join(separator)) if start_index.present?

      check_path(XLS_PATH)

      @counts =  {
          :files => 0,
          :objects => 0
      }

      if from_ftp

         import_from_ftp(start_index: start_index, limit: limit, only_count: only_count, is_power: is_power)
      else

         import_from_local(start_index: start_index, limit: limit,is_gas: is_gas, is_power: is_power, testing: testing, is_field: is_field)
      end

      if is_field
         copy_field_passports(from_ftp: from_ftp)
         generate_json_for_area_type(3, from_ftp: from_ftp)
      elsif is_power
         generate_json_for_area_type(2, from_ftp: from_ftp)
#         generate_json_for_area_type(2, from_ftp: from_ftp)
      else
         generate_invest_passports(from_ftp: from_ftp)
         generate_json_for_area_type(1, from_ftp: from_ftp)
         generate_addresses_json(from_ftp: from_ftp)

         prepare_invest_properties_data
      end

      generate_common_json(from_ftp: from_ftp)

      @logger.info(logger_texts[:end])
   end


   # @example InvestMap.get_full_statistic
   # @example InvestMap.get_full_statistic(result_to_file: true)
   #
   # Метод собирает полную файловую статистику по всем каталогам
   # кол-во файлов (успешных, ошибочных, не пройденных, игнорированных)
   # кол-во объектов (успешных, ошибочных, не пройденных)
   #
   # @param result_to_file[Boolean] - признак записать ли результат в файл, иначе пишет в лог
   #
   # @return
   #
   def self.get_full_statistic(result_to_file: false)

      @logger.info("Начат процесс сбора статистики объектов по файлам")
      if detect_from_ftp(ROOT_FTP_FOLDER)
         puts "Работа по FTP не реализована"
         @logger.info("Работа по FTP не реализована")

      else
         time_start = Time.now
         start_path = ["/mnt/ftp", ROOT_FTP_FOLDER].join('/')
         start_path_power = [start_path, POWER_FTP_FOLDER].join('/')
         start_path_field = [start_path, FIELD_FTP_FOLDER].join('/')

         dirs = []
         dirs.concat(get_directories(INVEST_FTP_FOLDER))
         dirs << start_path_power
         dirs << start_path_field

         file_count = find_files(start_path, full: true).count
         binding_progressbar = get_progressbar({is_ftp:false, total:file_count, length: 240})

         statistic = []

         begin
            dirs.each do |dir|

               get_statistic(dir, binding_progressbar)
               dir = dir.split('/').last if dir.include?("/mnt/ftp")
               statistic << {
                   district: dir,
                   stat: @pass_stat
               }
            end

         rescue => error
            @logger.warn(error)
         end

         past_time = Time.now - time_start
         @logger.info("Сбор полной статистики занял: #{seconds_to_units(past_time)}")

         if result_to_file
            puts_full_statistic_to_file(statistic)

         else
            puts statistic
            @logger.info(statistic)
         end

      end
   end


   # example InvestMap.get_statistic('БурзЯнский (р-н)')
   #
   # Метод собирает полную файловую статистику по заданному каталогу
   # кол-во файлов (успешных, ошибочных, не пройденных, игнорированных)
   # кол-во объектов (успешных, ошибочных, не пройденных)
   #
   # @param main_folder[String] - каталог в котором осуществлять поиск
   #    (должен находиться в корне root_folder), если не задан ведет поиск по всем каталогам.
   # @param binding_progressbar[ProgressBar] - инициализированный прогрессбар,
   #    если не задан инициализируется внутри.
   #
   # @return
   #
   def self.get_statistic(main_folder = nil, binding_progressbar = nil)

      # logger_texts = LOGGER_TEXTS
      # @logger.info("Начат процесс сбора статистики объектов по каталогу #{main_folder}")
      # time_start = Time.now

      if main_folder.present?
         if detect_from_ftp(ROOT_FTP_FOLDER)
            puts "Не реализовано"

         else
            if main_folder.include?('/mnt/ftp/')
               start_path = main_folder
            else
               start_path = "/mnt/ftp/#{ROOT_FTP_FOLDER}/#{INVEST_FTP_FOLDER}/#{main_folder}"
            end

            files = []
            files.concat(find_files(start_path, full: true))

            unless binding_progressbar.present?
               binding_progressbar = get_progressbar({is_ftp:false, total:files.count})
            end


            read_files_for_statistic(files, binding_progressbar)
         end

         # past_time = Time.now - time_start
         # @logger.info(["Сбор статистики занял:", seconds_to_units(past_time)].join(" "))
         # @logger.info(logger_texts[:footer])

      else
         puts "Основной каталог не задан"
      end
   end

   # Метод читает список переданных файлов, определяет количество объектов
   #  в каждом и суммирует их в соответствующем суффиксу файла значении @pass_stat
   #
   # @param working_files[Array] - массив путей к файлам.
   # @param binding_progressbar[ProgressBar] - инициализированный прогрессбар.
   #
   # @return
   #
   def self.read_files_for_statistic(working_files, binding_progressbar)

      clear_pass_stat

      working_files.each do |curr_file|

         if curr_file.include?('/ERR/')
            @pass_stat[:files][:error_info] += 1

         else

            current_handle_file = "обрабатывается файл: #{curr_file}"
            binding_progressbar.title = "#{ap current_handle_file}"
            binding_progressbar.format = "%t |%B| [%f] %p%%  Обработано %c/%C файлов"

            begin

               @pass_stat[:files][:count] += 1

               if curr_file.include?('_ERRORS')
                  @pass_stat[:files][:error] += 1

               elsif curr_file.include?('_COMPLETE')
                  @pass_stat[:files][:success] += 1

               elsif curr_file.include?('_service')
                  @pass_stat[:files][:excluded] += 1

               else
                  @pass_stat[:files][:not_processed] += 1
               end

               file_info = copy_and_read_local_file_for_statistic(curr_file)

               total_count = file_info[:count]
               if total_count.present?
                  @pass_stat[:objects][:count] += total_count

                  if curr_file.include?('_COMPLETE')
                     @pass_stat[:objects][:success] += total_count

                  elsif curr_file.include?('_ERRORS')
                     @pass_stat[:objects][:error] += total_count

                  else
                     @pass_stat[:objects][:not_processed] += total_count
                  end

               else
                  @logger.info("#{curr_file}: Результат неизвестен")
               end

            rescue Exception => ex
               @pass_stat[:files][:read_error] += 1
               @logger.error("Ошибка чтения файла: #{curr_file}: #{ex}")
            end

         end

         binding_progressbar.increment
      end

   end


   # Метод читает содержимое указанного файла и возвращает его в виде хэша с данными
   #
   # @param [String] file_path - путь к файлу
   #
   # @return [Hash] - хэш с содержимым файла
   #
   def self.copy_and_read_local_file_for_statistic(file_path)

      xls_path = XLS_PATH

      ext = file_path.include?('.xlsx') ? 'xlsx' : 'xls'
      local_file_name = ['processing-stat', ext].join('.')
      local_file_name = check_local_file_name(local_file_name)

      file_info = {}

      begin

         FileUtils.cp(file_path, [xls_path , local_file_name].join('/'))
         file_info = processing_invest_map_data_for_statistic(local_file_name)

      rescue Ole::Storage::FormatError
         # Изменяем формат файла и пробуем прочитать снова
         ext = file_path.include?('.xlsx') ? 'xls' : 'xlsx'
         local_file_name = ['processing-stat', ext].join('.')
         @logger.info("#{file_path} Неверный формат файла. Пробуем прочитать в формате: #{ext}")

         FileUtils.cp(file_path, [xls_path , local_file_name].join('/'))
         begin
            file_info = processing_invest_map_data_for_statistic(local_file_name)

         rescue Ole::Storage::FormatError
            @logger.info "Неверный формат файла: #{file_path}"

         rescue Exception => ex
            @logger.error("Ошибка обработки файла: #{file_path}: #{ex}")
         end

      rescue Exception => ex
         @logger.error("Ошибка обработки файла: #{file_path}: #{ex}")
      end

      Dir.glob("#{xls_path}/#{local_file_name}").each { |file| File.delete(file)}

      file_info
   end


   # Метод читает содержимое указанного файла и возвращает его в виде хэша с данными
   #
   # @param [String] file_name - имя файла
   #
   # @return [Hash] - хэш с содержимым файла
   #
   def self.processing_invest_map_data_for_statistic(file_name)
      xls_path = XLS_PATH

      file_path = [xls_path , file_name].join('/')
      begin
      book = Roo::Spreadsheet.open(file_path)
         rescue byebug
      end

      # Получаем массив (array) вкладок документа xls
      sheets = book.sheets

      result_of_sheets_handle = get_objects_count_from_sheets(book, sheets)

      objects_count = result_of_sheets_handle[:count]

      result = {
         objects: result_of_sheets_handle[:objects],
         count: objects_count
      }

      result
   end



   def self.get_objects_count_from_sheets(xls, sheets)

      inventory_obj_list = []

      sheet_name = nil
      sheets.each do |sheet|
         sheet_name = OBJECTS_INFO if sheet.eql?("Объекты (ЗУ и ОКС)")
         sheet_name = CAPACITY if sheet.eql?("Мощности")
         sheet_name = FIELD_INFO if sheet.eql?("Месторождения")
      end

      # Парсим вкладки xls
      list = get_invest_object_list_from_sheet(xls, sheets, sheet_name)

      inventory_obj_list.concat(list)
      objects_count = list.count

      {
         objects: inventory_obj_list,
         count: objects_count
      }
   end


   # Загружает список инвест объектов с указанной вкладки xls
   #
   # @param book[XLS] - xls-файл
   # @param sheets[Array] - массив вкладок с xls-файла
   # @param sheet_name[String] - наименование вкладки, с которой нужно считывать данные
   #
   # @return [Array] - список инвест объектов
   #
   def self.get_invest_object_list_from_sheet(book, sheets, sheet_name)

      sheet_num = get_num_sheet_by_name(sheets, sheet_name)
      sheet = book.sheet(sheet_num)

      rows = sheet.parse
      row_number = 0

      exam_card_list = []

      first_read_row_number = 4 if sheet_name.eql?(OBJECTS_INFO)
      first_read_row_number = 2 if sheet_name.eql?(CAPACITY)
      first_read_row_number = 5 if sheet_name.eql?(FIELD_INFO)

      rows.each do
         row_number += 1
         next if row_number < first_read_row_number

         if sheet.cell('A', row_number).to_s.present?
            if sheet_name.eql?(OBJECTS_INFO) || sheet_name.eql?(CAPACITY) || sheet_name.eql?(FIELD_INFO)
               exam_card = sheet.cell('A', row_number)
               exam_card_list << exam_card
            end
         end

      end

      exam_card_list
   end


   # Получаем номер (индекс) вкладки по ее имени
   #
   # @param sheets[Array] - массив вкладок xls-документа.
   # @param sheet_name[String] - имя вкладки
   #
   # @return [fixnum] num_sheet - Номер вкладки
   #
   def self.get_num_sheet_by_name(sheets, sheet_name)
      result_sheets = []
      sheets.each do |name|
         result_sheets << name.strip
      end
      num_sheet = result_sheets.index(sheet_name)
      num_sheet.present? && num_sheet >= 0 ? num_sheet : -1
   end


   # Метод возвращает список директорий в корне переданной
   # или в ROOT_FTP_FOLDER, если main_folder не задана
   # Работает только при локальной работе.
   #
   # @param main_folder[String] - имя директории
   #
   # @return [Array] - список директорий
   #
   def self.get_directories(main_folder = nil)

      unless detect_from_ftp(ROOT_FTP_FOLDER, true)

         start_path = "/mnt/ftp/#{ROOT_FTP_FOLDER}"
         start_path += "/#{main_folder}" if main_folder.present?

         find_directories(start_path)
      end
   end


   # Вызывает метод переименования файлов в указанной директории, а также во всех
   # ее дочерних директориях (убирает суффиксы _COMPLETE и _ERRORS в именах файлов),
   # также в процессе переименования файлов запускается очистка каталога ERR
   #
   # @param [String] main_folder - каталог в котором осуществлять поиск
   # @param [Boolean] only_errors - признак, переименовывать только файлы ошибок.
   #
   def self.rename_files(main_folder: nil, only_errors: false)
      rename_files_on_ftp(root_folder: ROOT_FTP_FOLDER, main_folder: main_folder, only_errors: only_errors)
   end


   # Находит соответствия инвестиционным объектам в таблице properties и заполняет
   # таблицу invest_area_properties
   #
   def self.prepare_invest_properties_data
      InvestArea.all.each do |obj|
         if obj[:cadastre_number].present?
            property = Property.where(real_cadastre_number: obj[:cadastre_number])

            if property.present?
               investAreaProperty = InvestAreaProperty.new
               investAreaProperty.invest_area_id = obj.id
               investAreaProperty.property_id = property.first[:id]
               investAreaProperty.save
            end
         end
      end
   end


   # example ImportPropertyObject.check_processed_files
   #
   # Метод собирает файловую статистику обработки файлов на FTP-сервере
   #
   def self.check_processed_files
      logger_texts = LOGGER_TEXTS

      @file_stat = {
         :count => 0,
         :success => 0,
         :error => 0,
         :excluded => 0,
         :file_error => 0
      }

      @logger.info(logger_texts[:footer])
      @logger.info(logger_texts[:start_error_statistic])
      time_start = Time.now

      if detect_from_ftp(ROOT_FTP_FOLDER)
         @logger.info([logger_texts[:mount_point], logger_texts[:not_finded]].join(' '))
         @ftp = ftp_connect
         @ftp.chdir(ROOT_FTP_FOLDER)

         explore_ftp_processed_files
         @ftp.close

      else
         @logger.info([logger_texts[:mount_point], logger_texts[:finded]].join(' '))
         start_path = "/mnt/ftp/#{ROOT_FTP_FOLDER}"
         explore_local_processed_files(start_path)
      end

      past_time = Time.now - time_start
      puts "Статистика: всего: #{@file_stat[:count]}, успешно: #{@file_stat[:success]}, не прошли ФЛК: #{@file_stat[:error]}, не обработано файлов: #{@file_stat[:file_error]}"
      @logger.info (["Статистика: всего:", @file_stat[:count],
                     "успешно:", @file_stat[:success], "не прошли ФЛК:", @file_stat[:error],
                     "не обработано файлов:", @file_stat[:file_error]].join(' '))

      @logger.info([logger_texts[:work_time], past_time, "секунд"].join(" "))
   end


   # Генерирует json-файл с адресами инвестиционных объектов для карты,
   # файл копируется на ftp-сервер
   #
   # @param from_ftp[Boolean] - режим работы с FTP (true) или локально (false)
   #
   # @return [File] - файл формата json с данными
   #
   def self.generate_addresses_json(from_ftp: nil)
      file_name = 'addresses'

      region_set = Set.new
      village_council_set = Set.new
      locality_set = Set.new

      invest_object_list = InvestArea.where(area_type: 1)

      invest_object_list.each do |object|
         if object[:address_hash]["region"]["simple_name"].present?
            region_set.add(object[:address_hash]["region"]["simple_name"])
         end

         if object[:address_hash]["village_council"]["simple_name"].present?
            village_council_set.add(object[:address_hash]["village_council"]["simple_name"])
         end

         if object[:address_hash]["locality"]["simple_name"].present?
            locality_set.add(object[:address_hash]["locality"]["simple_name"])
         end
      end

      region_set.delete('0.0')

      addr_hash = {
         regions: region_set.to_a,
         sovets: village_council_set.to_a,
         living_areas: locality_set.to_a,
         administrations: []
      }

      put_json_to_file(file_name, addr_hash, from_ftp: from_ftp)
   end


   # Генерирует json-файлы с данными инвестиционных объектов и мощностей для карты,
   # файлы копируются на ftp-сервер
   #
   # @param area_type[Integer] - Код типа объекта (1 - инвест. объект, 2 - мощность)
   # @param from_ftp[Boolean] - режим работы с FTP (true) или локально (false)
   #
   # @return [File] - файл формата json с данными
   #
   def self.generate_json_for_area_type(area_type, from_ftp: nil)
      file_name = "objects"
      object_type = OBJECTS_AREA_TYPE

      if area_type == 2
         file_name = "powers"
         object_type = POWER_AREA_TYPE
      end

      if area_type == 3
         file_name = "resources"
         object_type = FIELD_AREA_TYPE
      end

      invest_object_list = InvestArea.where(area_type: area_type)
      inv_objects = generate_invest_object_json(invest_object_list, object_type)

      put_json_to_file(file_name, inv_objects, from_ftp: from_ftp)
   end


   # Генерирует json-файл (общий) с данными для инвест портала
   # InvestMap.generate_common_json(from_ftp: false)
   #
   # @param from_ftp[Boolean] - режим работы с FTP (true) или локально (false)
   #
   # @return [File] - файл формата json с данными
   def self.generate_common_json(from_ftp: nil)
      invest_area_list = InvestArea.all

      invest_object_list = []
      power_object_list = []
      field_object_list = []

      invest_area_list.each do |invest_area|
         addr = get_polygon_info(invest_area)

         begin

            if (addr.to_s.exclude?("район") & (addr.present?)&(addr[addr.length-2..addr.length-1].eql?("ий")))
               addr = addr + " район"
            else
               if (addr.to_s.include?("жгорье") & addr.to_s.exclude?("ородской"))
                  addr = "Городской округ ЗАТО " + addr
               else
                  addr = "Городской округ " + addr
               end
            end

         rescue

         end

         if invest_area[:area_type] == 1     # инвест объект

            invest_object = prepare_invest_object_json_part(invest_area, addr)
            invest_object_list << prepare_suggestions_for_using_list(invest_object)
            invest_object_list << prepare_property_types_list(invest_object)


         elsif invest_area[:area_type] == 2  # мощность

            power_object_list << prepare_power_object_json_part(invest_area, addr)

         elsif invest_area[:area_type] == 3  # месторождение

            field_object_list << prepare_field_object_json_part(invest_area, addr)

         end
      end

      result_hash = {
         investObjects: invest_object_list,
         electicSubstations: power_object_list,
         landResources: field_object_list,
      }


      file_name = "common"
      put_json_to_file(file_name, result_hash, from_ftp: from_ftp)
      put_data_to_xls(result_hash, from_ftp: from_ftp)
   end



   # InvestMap.generate_common_json(from_ftp: false)
   # Формирует файл формата xls (common.xls) с данными для инвест портала
   #
   # @param from_ftp[Boolean] - режим работы с FTP (true) или локально (false)
   #
   # @return [File] - файл формата xls с данными
   def self.put_data_to_xls(result_hash, from_ftp: nil)

      xls_path = XLS_PATH
      check_path(xls_path)

      Spreadsheet.client_encoding = 'UTF-8'
      book = Spreadsheet::Workbook.new

      stat_sheet = book.create_worksheet(:name => 'Инвест_объекты')
      prepare_invest_data_for_xls(result_hash, stat_sheet)

      stat_sheet = book.create_worksheet(:name => 'Мощности')
      prepare_power_data_for_xls(result_hash, stat_sheet)

      stat_sheet = book.create_worksheet(:name => 'Месторождения')
      prepare_field_data_for_xls(result_hash, stat_sheet)

      xls_file_name = "common.xls"
      file_path = [XLS_PATH, xls_file_name].join('/')
      book.write(file_path)

      move_common_xls_to_ftp(xls_file_name, from_ftp: from_ftp)

      true
   end


   def self.prepare_field_data_for_xls(result_hash, stat_sheet)
      invest_list = result_hash[:landResources]

      stat_sheet[0, 0] =  "Месторождение"
      stat_sheet[0, 1] =  "Координата X"
      stat_sheet[0, 2] =  "Координата Y"
      stat_sheet[0, 3] =  "Ресурс"
      stat_sheet[0, 4] =  "Адрес"
      stat_sheet[0, 5] =  "Лицензия"
      stat_sheet[0, 6] =  "Резервы A+B+C1"
      stat_sheet[0, 7] =  "Резервы C2"
      stat_sheet[0, 8] =  "Забалансный запас"
      stat_sheet[0, 9] =  "Паспорт месторождения"
      stat_sheet[0, 10] =  "Район"
      stat_sheet[0, 11] =  "Распределенный"
      stat_sheet[0, 12] =  "Ед. измерения"

      row_number = 1
      invest_list.each do |obj|
         stat_sheet[row_number, 0] =  obj[:name][:value][:rus]
         stat_sheet[row_number, 1] =  obj[:coordinates][0]
         stat_sheet[row_number, 2] =  obj[:coordinates][1]
         stat_sheet[row_number, 3] =  obj[:data][:resource][:value][:rus]
         stat_sheet[row_number, 4] =  obj[:data][:address][:value][:rus]
         stat_sheet[row_number, 5] =  obj[:data][:license][:value][:rus]
         stat_sheet[row_number, 6] =  obj[:data][:abc1][:value]
         stat_sheet[row_number, 7] =  obj[:data][:c2][:value]
         stat_sheet[row_number, 8] =  obj[:data][:offsheet][:value]
         stat_sheet[row_number, 9] =  obj[:data][:passport][:value][:rus]
         stat_sheet[row_number, 10] =  obj[:data][:district][:value][:rus]
         stat_sheet[row_number, 11] =  obj[:data][:distributed][:value]
         stat_sheet[row_number, 12] =  obj[:unit][:value][:rus]

         row_number = row_number + 1
      end
   end


   def self.prepare_power_data_for_xls(result_hash, stat_sheet)
      invest_list = result_hash[:electicSubstations]

      stat_sheet[0, 0] =  "Электроподстанция"
      stat_sheet[0, 1] =  "Координата X"
      stat_sheet[0, 2] =  "Координата Y"
      stat_sheet[0, 3] =  "Адрес"
      stat_sheet[0, 4] =  "Мощность по договорам"
      stat_sheet[0, 5] =  "Мощность по замерам"
      stat_sheet[0, 6] =  "Резерв мощности"
      stat_sheet[0, 7] =  "Кол-во заявок"
      stat_sheet[0, 8] =  "Сроки реконструкции"
      stat_sheet[0, 9] =  "Район"
      stat_sheet[0, 10] =  "Ед. измерения"


      row_number = 1
      invest_list.each do |obj|
         stat_sheet[row_number, 0] =  obj[:name][:value][:rus]
         stat_sheet[row_number, 1] =  obj[:coordinates][0]
         stat_sheet[row_number, 2] =  obj[:coordinates][1]
         stat_sheet[row_number, 3] =  obj[:data][:address][:value][:rus]
         stat_sheet[row_number, 4] =  obj[:data][:contract][:value]
         stat_sheet[row_number, 5] =  obj[:data][:metered][:value]
         stat_sheet[row_number, 6] =  obj[:data][:reserve][:value]
         stat_sheet[row_number, 7] =  obj[:data][:request][:value][:rus]
         stat_sheet[row_number, 8] =  obj[:data][:rebuild][:value][:rus]
         stat_sheet[row_number, 9] =  obj[:data][:district][:value][:rus]
         stat_sheet[row_number, 10] =  obj[:unit][:value][:rus]

         row_number = row_number + 1
      end
   end


   def self.prepare_invest_data_for_xls(result_hash, stat_sheet)
      invest_list = result_hash[:investObjects]

      stat_sheet[0, 0] =  "Инвестиционный объект"
      stat_sheet[0, 1] =  "Координата X"
      stat_sheet[0, 2] =  "Координата Y"
      stat_sheet[0, 3] =  "Телефон"
      stat_sheet[0, 4] =  "Сайт"
      stat_sheet[0, 5] =  "Адрес"
      stat_sheet[0, 6] =  "E-mail"
      stat_sheet[0, 7] =  "Имя файла инвест. паспорта (рус)"
      stat_sheet[0, 8] =  "Имя файла инвест. паспорта (англ)"
      stat_sheet[0, 9] =  "Район"
      stat_sheet[0, 10] =  "Газ"
      stat_sheet[0, 11] =  "Водопровод"
      stat_sheet[0, 12] =  "Электричество"
      stat_sheet[0, 13] =  "Очистные сооружения"
      stat_sheet[0, 14] =  "Канализация"
      stat_sheet[0, 15] =  "Площадь ОКС на ЗУ"
      stat_sheet[0, 16] =  "Площадь ЗУ"
      stat_sheet[0, 17] =  "Расстояние до ж/д"
      stat_sheet[0, 18] =  "Расстояние до автомагистрали"
      stat_sheet[0, 19] =  "Фото 1"
      stat_sheet[0, 20] =  "Фото 1"
      stat_sheet[0, 21] =  "Фото 1"
      stat_sheet[0, 22] =  "Фото 1"

      row_number = 1
      invest_list.each do |obj|
         stat_sheet[row_number, 0] =  obj[:name][:value][:rus]
         stat_sheet[row_number, 1] =  obj[:coordinates][0]
         stat_sheet[row_number, 2] =  obj[:coordinates][1]
         stat_sheet[row_number, 3] =  obj[:data][:phone][:value]
         stat_sheet[row_number, 4] =  obj[:data][:site][:value]
         stat_sheet[row_number, 5] =  obj[:data][:address][:value][:rus]
         stat_sheet[row_number, 6] =  obj[:data][:email][:value]
         stat_sheet[row_number, 7] =  obj[:data][:passport][:value][:rus]
         stat_sheet[row_number, 8] =  obj[:data][:passport][:value][:eng]
         stat_sheet[row_number, 9] =  obj[:data][:district][:value][:rus]
         stat_sheet[row_number, 10] =  obj[:data][:gas][:value]
         stat_sheet[row_number, 11] =  obj[:data][:water][:value]
         stat_sheet[row_number, 12] =  obj[:data][:electric][:value]
         stat_sheet[row_number, 13] =  obj[:data][:recycle][:value]
         stat_sheet[row_number, 14] =  obj[:data][:sewerage][:value]
         stat_sheet[row_number, 15] =  obj[:data][:oks][:value]
         stat_sheet[row_number, 16] =  obj[:data][:zy][:value]
         stat_sheet[row_number, 17] =  obj[:data][:railway][:value]
         stat_sheet[row_number, 18] =  obj[:data][:highway][:value]
         stat_sheet[row_number, 19] =  obj[:gallery][0][:path]
         stat_sheet[row_number, 20] =  obj[:gallery][1][:path]
         stat_sheet[row_number, 21] =  obj[:gallery][2][:path]
         stat_sheet[row_number, 22] =  obj[:gallery][3][:path]

         row_number = row_number + 1
      end
   end


   def self.move_common_xls_to_ftp(xls_file_name, from_ftp: nil)
      xls_path = XLS_PATH
      worked_file = [xls_path, xls_file_name].join('/')


      if from_ftp
         target_path = [ROOT_FTP_FOLDER, TARGET_FTP_FOLDER].join('/')
         check_ftp_connection
         begin
            @ftp.putbinaryfile(worked_file, [target_path, xls_file_name].join('/'))
            Dir.glob(xls_path + '/*.txt').each { |file| File.delete(file)}
         rescue
            @logger.warn("Ошибка перемещения файла на FTP-сервер")
         end
      else
         target_path = "/mnt/ftp/#{ROOT_FTP_FOLDER}/#{TARGET_FTP_FOLDER}"

         move_file(worked_file, [target_path, xls_file_name].join('/'))

         unless File.exist?([target_path, xls_file_name].join('/'))
            @logger.warn("Ошибка копирования файла")
         end
      end

   end




   # Трансформирует переданное значение в булевский тип ('да' в true, остальные
   # значения в false)
   #
   # @param value[String] - значение
   #
   # #return [Boolean]
   #
   def self.get_available_infrastructure(value)
      return value.mb_chars.downcase.to_s.eql?('да') ? true : false
   end


   # Получает 2 атрибута инвест. объекта на рус. и англ. языке и формирует их в виде хэша
   #
   # @param rus_value[String] - значение атрибута инвест. объекта на русс. языке
   # @param eng_value[String] - значение атрибута инвест. объекта на англ. языке
   #
   # @return [Hash]. Содержимое
   #             :rus [String] - значение поля на рус. языке
   #             :eng [String] - значение поля на англ. языке
   #
   def self.get_value_hash(rus_value, eng_value, check_numeric: false)

      return {
         rus: format_numeric_value_for_json(rus_value, check_numeric), #rus_value.present? ? rus_value : "-",
         eng: format_numeric_value_for_json(eng_value, check_numeric)  #eng_value.present? ? eng_value : "-"
      }
   end


   # Формирует часть общего json-файла (раздел атрибутов)
   #
   # @param rus_text[String] - наименование атрибута инвест. объекта на русс. языке
   # @param eng_text[String] - наименование атрибута инвест. объекта на англ. языке
   # @param value[String/Hash] - значение атрибута инвест. объекта в виде строки или в виде хэша
   # @param is_show[Boolean] - флаг, отображать данный атрибут на карте (true) или нет (false)
   # @param icon[String] - имя иконки
   # @param type[String] - тип атрибута (строка, число, email и т.д)
   # @param check_numeric[String] - флаг, преобразовывать ли полученное значение в число (true)
   #
   # @return [Hash]. Содержимое
   #          text: {
   #                  rus: [String] - значение поля на рус. языке
   #                  eng: [String] - значение поля на англ. языке
   #                },
   #          value: [String/Hash] - переданно значение value,
   #          show: [Boolean] - значение флага, отображать ли данный атрибут на карте,
   #          icon: [String] - переданное значение icon
   #          type: [String] - переданное значение type
   #
   def self.prepare_json_part(rus_text, eng_text, value, is_show = false, icon = nil, type = nil, check_numeric = false)
      hash =  {
         text: {
            rus: rus_text,
            eng: eng_text
         },
         value: format_hash_value_for_json(value, check_numeric)
      }

      hash[:show] = is_show
      hash[:icon] = icon if icon.present?
      hash[:type] = type if type.present?

      return hash
   end


   # Проверяет тип переданного значения. Если переданное значение является хэшом, то
   # возвращает его, в противном случае пытается преобразовать полученное значение в число
   #
   # @param value[String] - значение
   # @param check_numeric[Boolean] - флаг, форматировать ли переданное значение в число
   #
   # @return [Numeric/String] - результат формитирования
   #
   def self.format_hash_value_for_json(value, check_numeric)

      if value.is_a?(Hash)
         return value
      else
         return format_numeric_value_for_json(value, check_numeric)
      end
   end


   # Форматирование переданного значения для общего json-файла (common).
   #
   # @param value[String] - значение
   # @param check_numeric[Boolean] - флаг, форматировать ли переданное значение в число
   #
   # @return [Numeric/String] - результат формитирования
   #
   def self.format_numeric_value_for_json(value, check_numeric)
      if value.present?
         if check_numeric
            return int_then_float_then_string(value)
         else
            return value
         end
      else
         return  "-"
      end
   end


   # Полученную строку (параметр value) пытается последовательно преобразовать
   # в целочисленный тип, а затем в дробный. Если преобразование не удалось, то возвращет
   # полученную строку в исходном состоянии
   #
   # @param value[String]
   #
   # @return [Fixnum/String]
   #
   def self.int_then_float_then_string(value)
      begin
         if value.to_i.to_s == value
            return value.to_i
         elsif value.to_f.to_s == value
            return value.to_f
         else
            return value
         end
      rescue
         return value
      end
   end


   # Формирует часть общего json-файла (раздел с информацией по фотографиям)
   #
   # @param photos[Array] - массив с информацией по фотографиям инвест. объекта
   #
   # @return [Array of Hash] - сформировнный массив хэшей с данными прикрепенных
   #                           фотографий
   #           id: [Fixnum] - id фотографии,
   #           path: [String] - путь, где расположена фотография,
   #           description: {
   #              rus: [String] - описание фото на рус. языке,
   #              eng: [String] - описание фото на англ. языке
   #           }
   #
   def self.prepare_gallery_hash(photos)

      photos_arr = []

      photos.each do |photo|
         photos_arr << {
            id: photo["id"],
            path: photo["phoenix_path"],
            description: {
               rus: photo["description_rus"],
               eng: photo["description_eng"]
            }

         }
      end

      return photos_arr
   end



   # Формирует часть общего json-файла (раздел с данными инвест. объекта)
   #
   # @param invest_area[Object]
   # @param addr[String] - район
   #
   # @return [Hash] - сформировнный хэш
   #
   def self.prepare_invest_object_json_part(invest_area, addr)
      addr_eng = ""
      addr_eng = DISTRICTS_NAMES[addr] if DISTRICTS_NAMES[addr].present?



      return {
         id: invest_area[:id],
         name: prepare_json_part("Инвестиционный объект",
                                 "Investment object",
                                 get_value_hash(invest_area[:details_hash]["area_name_rus"],
                                                invest_area[:details_hash]["area_name_eng"])),
         coordinates: [
            invest_area[:details_hash]["cord_x"], invest_area[:details_hash]["cord_y"]
         ],
         data: {
            phone: prepare_json_part("Контактный телефон",
                                     "Contact phone",
                                     invest_area[:contacts_hash]["phones"][0]["phone_num"],
                                     true, "phone", "phone"),

            site: prepare_json_part("Сайт организации",
                                    "Site",
                                    invest_area[:contacts_hash]["site"],
                                    true, "site", "link"),

            address: prepare_json_part("Адрес", "Address", get_value_hash(invest_area[:details_hash]["address"], "-"), true, "address", "text"),

            ownership_type: prepare_json_part("Вид собственности","Ownership type", get_value_hash(invest_area[:details_hash]["ownership_type"],"-"),true,"ownership","text"),

            invest_area_type: prepare_json_part("Тип площадки", "Area type", get_value_hash(invest_area[:details_hash]["invest_area_type_name"],
                                                                                            translate_filtered_fields(3, invest_area[:details_hash]["invest_area_type_name"])),true,"invest_area_type_name","text"),

            area_condition: prepare_json_part("Общее текущее состояние объекта", "Area condition", get_value_hash(invest_area[:details_hash]["current_state"],
                                                                                                                  translate_filtered_fields(2, invest_area[:details_hash]["current_state"])),true,"condition","text"),
            email: prepare_json_part("Почтовый адрес",
                                     "Email",
                                     invest_area[:contacts_hash]["email"],
                                     true, "email", "email"),

            passport: prepare_json_part("Паспорт объекта",
                                        "Investment passport",
                                        get_value_hash(invest_area[:details_hash]["pdf_file_name"],
                                                       invest_area[:details_hash]["pdf_file_name_eng"]),
                                        true, "passport", "passport"),

            district: prepare_json_part("Район", "District", get_value_hash(addr, addr_eng), false, "", "text"),

            gas: prepare_json_part("Газ", "Gas", get_available_infrastructure(invest_area[:details_hash]["gas_available"]), false, "", "text"),
            water: prepare_json_part("Водопровод", "Water", get_available_infrastructure(invest_area[:details_hash]["water_available"]), false, "", "text"),
            electric: prepare_json_part("Электричество", "Electricity", get_available_infrastructure(invest_area[:details_hash]["electric_available"]), false, "", "text" ),
            recycle: prepare_json_part("Очистные сооружения", "Recycle", get_available_infrastructure(invest_area[:details_hash]["treatment_facilities_available"]), false, "", "text"),
            sewerage: prepare_json_part("Канализация", "Sewerage", get_available_infrastructure(invest_area[:details_hash]["sewerage_description"]), false, "", "text"),

            "#{translate_filtered_fields(0, invest_area[:details_hash]["use_types"])}": prepare_json_part("#{invest_area[:details_hash]["use_types"]}",
                                                                                             "#{translate_filtered_fields(0, invest_area[:details_hash]["use_types"])}",true,false,"","text"),

            "#{translate_filtered_fields(1, invest_area[:details_hash]["ownership_type"])}": prepare_json_part("#{invest_area[:details_hash]["ownership_type"]}",
                                                                                                         "#{translate_filtered_fields(1, invest_area[:details_hash]["ownership_type"])}",true,false,"","text"),
            oks: prepare_json_part("Площадь ОКС на ЗУ", "CBO area footage",
                                   invest_area[:details_hash]["total_oks_footage"], false, "", "num", true),

            zy: prepare_json_part("Площадь земельного участка", "Area footage",
                                  invest_area[:details_hash]["total_land_footage"], false, "", "num", true),

            railway: prepare_json_part("Расстояние до Ж/Д", "Distance to railway",
                                       invest_area[:details_hash]["rails_distance"], false, "", "num", true),

            highway: prepare_json_part("Расстояние до автомагистрали", "Distance to highway",
                                       invest_area[:details_hash]["auto_distance"], false, "", "num", true),

            # footeage: prepare_json_part("Площадь", "Area footage", invest_area[:details_hash]["total_land_footage"], false),
         },

         gallery: prepare_gallery_hash(invest_area[:details_hash]["photos"])
      }
   end


   # Формирует часть общего json-файла (раздел с данными мощностей)
   #
   # @param invest_area[Object]
   # @param addr[String] - район
   #
   # @return [Hash] - сформировнный хэш
   #
   def self.prepare_power_object_json_part(invest_area, addr)
      addr_eng = ""
      addr_eng = DISTRICTS_NAMES[addr] if DISTRICTS_NAMES[addr].present?
      return {
         id: invest_area[:id],
         name: prepare_json_part("Электроподстанция",
                                 "Electrical substation",
                                 get_value_hash(invest_area[:area_name],
                                                invest_area[:area_name]),
                                 false),
         coordinates: [
            invest_area[:details_hash]["cord_x"], invest_area[:details_hash]["cord_y"]
         ],
         data: {
            address: prepare_json_part("Адрес", "Address", get_value_hash(invest_area[:details_hash]["address_string"], "-"), true, "address", "text"),

            contract: prepare_json_part("Мощность по договорам",
                                        "Contract power",
                                        invest_area[:details_hash]["contract_power"],
                                        true, "contract", "num", true),

            metered: prepare_json_part("Мощность по замерам",
                                       "Metered power",
                                       invest_area[:details_hash]["metered_power"],
                                       true, "metered", "num", true),

            reserve: prepare_json_part("Резерв мощности",
                                       "Reserve power",
                                       invest_area[:details_hash]["reserve_power"],
                                       true, "reserve", "num", true),

            request: prepare_json_part("Количество заявок (заключённых договоров)",
                                       "Sent and approved requests",
                                       get_value_hash(invest_area[:details_hash]["request_count"].to_i.to_s,
                                                      invest_area[:details_hash]["request_count"].to_i.to_s),
                                       true, "request", "text"),

            rebuild: prepare_json_part("Сроки реконструкции",
                                       "Rebuild time",
                                       get_value_hash(invest_area[:details_hash]["rebuild_time"],
                                                      invest_area[:details_hash]["rebuild_time"]),
                                       true, "rebuild", "text"),

            district: prepare_json_part("Район", "District", get_value_hash(addr, addr_eng), false, "", "text"),
         },

         unit: prepare_json_part("Единица измерения", "Unit", get_value_hash("кВт", "kw"))
      }
   end



   # Формирует часть общего json-файла (раздел с данными месторождений)
   #
   # @param invest_area[Object]
   # @param addr[String] - район
   #
   # @return [Hash] - сформировнный хэш
   #
   def self.prepare_field_object_json_part(invest_area, addr)
      addr_eng = ""
      addr_eng = DISTRICTS_NAMES[addr] if DISTRICTS_NAMES[addr].present?
      return {
         id: invest_area[:id],
         name: prepare_json_part("Месторождение",
                                 "Resource",
                                 get_value_hash(invest_area[:area_name],
                                                invest_area[:details_hash]["area_name_eng"])),
         coordinates: [
            invest_area[:details_hash]["cord_x"], invest_area[:details_hash]["cord_y"]
         ],
         data: {
            resource: prepare_json_part("Ресурс",
                                        "Resource",
                                        get_value_hash(invest_area[:details_hash]["use"],
                                                       invest_area[:details_hash]["use_eng"]),
                                        true, "resource", "text"),

            address: prepare_json_part("Адрес", "Address", get_value_hash(invest_area[:details_hash]["address_string"], "-"), true, "address", "text"),

            license: prepare_json_part("Лицензия",
                                       "License",
                                       get_value_hash(invest_area[:details_hash]["license"],
                                                      invest_area[:details_hash]["license_eng"]),
                                       true, "license", "text"),

            abc1: prepare_json_part("Резервы A+B+C1",
                                    "Reserves A+B+C1",
                                    invest_area[:details_hash]["abc1_stock_value"],
                                    true, "abc1", "num", true),

            c2: prepare_json_part("Резервы C2",
                                  "Reserces C2",
                                  invest_area[:details_hash]["c2_stock_value"],
                                  true, "c2", "num", true),

            offsheet: prepare_json_part("Забалансный запас",
                                        "Off balance sheet",
                                        invest_area[:details_hash]["off_sheet_stock_value"],
                                        true, "ofsheet", "num", true),

            passport: prepare_json_part("Паспорт месторождения",
                                        "Field passport",
                                        get_value_hash(invest_area[:details_hash]["passport_file"],
                                                       invest_area[:details_hash]["passport_file"]),
                                        true, "passport", "passport"),

            district: prepare_json_part("Район", "District", get_value_hash(addr, addr_eng), false, "", "text"),

            distributed: prepare_json_part("Распределенный",
                                           "Distributed",
                                           is_distributed_field(invest_area[:details_hash]["is_distributed"]),
                                           false, "", "text"),
         },

         unit: prepare_json_part("Единица измерения",
                                 "Unit",
                                 get_value_hash(invest_area[:details_hash]["type_unit"],
                                                invest_area[:details_hash]["type_unit"])),

      }
   end


   # Трансформирует переданное значение "да" в true, а "нет" в false и возвращает его
   #
   # @param is_distributed_value[String] - значение поля "Распределенный".
   #
   # @return [Boolean]
   #
   def self.is_distributed_field(is_distributed_value)
      if is_distributed_value.present?
         return true if is_distributed_value.mb_chars.downcase.to_s.eql?('да')
      end

      return false
   end




   # Формирует хэш с данными инвестиционных объектов для отображения на карте
   #
   # @param invest_object_list[Array] - массив инвест. объектов из БД
   # @param area_type[Integer] - тип объекта
   #
   # @return [Hash] - сформированный хэш (структура зависит от типа переданного
   #                  объекта)
   #
   def self.generate_invest_object_json(invest_object_list, area_type)
      if area_type.eql?(OBJECTS_AREA_TYPE)
         obj =  {
            inv_objects_ids: prepare_invest_object_id_array_for_invest_object_json(invest_object_list),
            inv_objects: prepare_invest_object_details_for_invest_object_json(invest_object_list)
         }
      elsif area_type.eql?(POWER_AREA_TYPE)
         obj = {
            powers_ids: prepare_invest_object_id_array_for_invest_object_json(invest_object_list),
            powers: prepare_power_details_for_invest_object_json(invest_object_list),
         }
      elsif area_type.eql?(FIELD_AREA_TYPE)
         obj = {
            resources_ids: prepare_invest_object_id_array_for_invest_object_json(invest_object_list),
            resources: prepare_fields_details_for_invest_object_json(invest_object_list),
         }
      end

      return obj
   end


   # Формирует блок details_hash для хэша с данными по мощностям (энергообъекты)
   #
   # @param invest_object_list[Array] - массив инвест. объектов из БД
   #
   # @return [Hash] - сформированный хэш
   #
   def self.prepare_power_details_for_invest_object_json(invest_object_list)
      details_hash = []

      object_count = 1
      invest_object_list.each do |object|
         details_hash << {
            object_count.to_s => {
               name: object[:area_name],
               x: object[:details_hash]["cord_x"],
               y: object[:details_hash]["cord_y"],

               contract_power: object[:details_hash]["contract_power"],
               metered_power: object[:details_hash]["metered_power"],
               reserve_power: object[:details_hash]["reserve_power"],
               request_power: object[:details_hash]["request_power"],
               rebuild_time: object[:details_hash]["rebuild_time"]
            }
         }

         object_count += 1
      end

      return details_hash
   end


   # Формирует блок details_hash для хэша с данными по месторождениям
   #
   # @param invest_object_list[Array] - массив инвест. объектов из БД
   #
   # @return [Hash] - сформированный хэш
   #
   def self.prepare_fields_details_for_invest_object_json(invest_object_list)
      details_hash = []

      object_count = 1
      invest_object_list.each do |object|
         details_hash << {
            object_count.to_s => {
               area_name: object[:area_name],
               area_name_eng: object[:details_hash]["area_name_eng"],
               use: object[:details_hash]["use"],
               license: object[:details_hash]["license"],
               license_eng: object[:details_hash]["license_eng"],
               type_unit: object[:details_hash]["type_unit"],
               abc1_stock_value: object[:details_hash]["abc1_stock_value"],
               c2_stock_value: object[:details_hash]["c2_stock_value"],
               off_sheet_stock_value: object[:details_hash]["off_sheet_stock_value"],
               x: object[:details_hash]["cord_x"],
               y: object[:details_hash]["cord_y"],
               passport_file: object[:details_hash]["pdf_file_name"],
               source_summary_table_path: object[:details_hash]["source_summary_table_path"]
            }
         }

         object_count += 1
      end

      return details_hash
   end


   # Подготовливает массив с псевдо id-значениями (счетчик) инвестиционных объектов
   #     для карты
   #
   # @param invest_object_list[Array] - массив инвест. объектов из БД
   #
   # @return [Array] - массив с псевдо id-значениями инвестиционных объектов
   #
   def self.prepare_invest_object_id_array_for_invest_object_json(invest_object_list)
      inv_obj_id_array = []

      object_count = 1
      invest_object_list.each do |object|
         inv_obj_id_array << object_count.to_s
         object_count += 1
      end

      return inv_obj_id_array
   end


   # Формирует блок details_hash для хэша с данными по инвестиционным объектам
   #
   # @param invest_object_list[Array] - массив инвест. объектов из БД
   #
   # @return [Array] - details_hash - сформированный массив с детальной информацией
   #                    по заданному инвестицонному объекту
   #
   def self.prepare_invest_object_details_for_invest_object_json(invest_object_list)
      target_path = [ROOT_FTP_FOLDER, TARGET_FTP_FOLDER].join('/')

      details_hash = {}

      object_count = 1

      invest_object_list.each do |invest_object|
         details_hash = details_hash.merge(
            object_count.to_s => {
               name: invest_object[:area_name],
               x: invest_object[:details_hash]["cord_x"],
               y: invest_object[:details_hash]["cord_y"],
               invest_passport: invest_object[:details_hash]["pdf_file_name"].present? ? [target_path, invest_object[:details_hash]["pdf_file_name"]].join('/') : '',
               invest_passport_eng: invest_object[:details_hash]["pdf_file_name_eng"].present? ? [target_path, invest_object[:details_hash]["pdf_file_name_eng"]].join('/') : '',
               phone: get_first_not_empty_phone_number(invest_object[:contacts_hash].present? ? invest_object[:contacts_hash]["phones"] :[]),
               site: invest_object[:contacts_hash].present? ? invest_object[:contacts_hash]["site"] : '',
               address: invest_object[:details_hash]["address"],
               email: invest_object[:contacts_hash].present? ? invest_object[:contacts_hash]["email"] : '',
               filter_params: {
                  invest_area_type: get_invest_area_type_name(invest_object, :rus),
                  region: get_address_object_from_hash(invest_object[:address_hash], :region),
                  living_area: get_address_object_from_hash(invest_object[:address_hash], :locality),
                  sovet: get_address_object_from_hash(invest_object[:address_hash], :village_council),
                  administration: nil,
                  gas: invest_object[:details_hash]["gas_available"].eql?('ДА') ? true : false,
                  water: invest_object[:details_hash]["water_available"].eql?('ДА') ? true : false,
                  electric: invest_object[:details_hash]["electric_available"].eql?('ДА') ? true : false,
                  water_recycle: invest_object[:details_hash]["treatment_facilities_available"].eql?('ДА') ? true : false,
                  sewerage: invest_object[:details_hash]["sewerage_available"].eql?('ДА') ? true : false,
                  area_footage: invest_object[:details_hash]["total_oks_footage"],
                  min_cost: invest_object[:details_hash]["min_cost"],
                  max_cost: invest_object[:details_hash]["max_cost"]
               },
               photos: prepare_photos_hash(invest_object[:details_hash]["photos"]),
         })
         object_count += 1
      end


      return details_hash
   end


   # Возвращает наименование адресного объекта по ключу
   #
   # @param address_hash[Hash] - хэш с адресными объектами
   # @param param[String] - ключ
   #
   # @return [String]
   #
   def self.get_address_object_from_hash(address_hash, param)
      if address_hash.present?
         if address_hash[param.to_s].present? && address_hash[param.to_s]["simple_name"].present?
            return address_hash[param.to_s]["simple_name"]
         end
      end

      return ''
   end


   # Генерирует инвест. паспорта (pdf) и копирует созданные файла на ftp-сервер
   #
   # @param from_ftp[Boolean] - режим работы с FTP (true) или локально (false)
   #
   def self.generate_invest_passports(from_ftp: nil)
      xls_path = XLS_PATH

      invest_object_list = InvestArea.where(area_type: 1)

      @rus = 0
      @eng = 0

      invest_object_list.each do |obj|
         generate_pdf(:rus, obj, xls_path, from_ftp: from_ftp)
         generate_pdf(:eng, obj, xls_path, from_ftp: from_ftp)
      end
   end



   #
   def self.copy_field_passports(from_ftp: nil)
      invest_object_list = InvestArea.where(area_type: 3)

      invest_object_list.each do |obj|
         remote_path_string = obj[:details_hash]["source_summary_table_path"]
         if from_ftp
            target_path = [ROOT_FTP_FOLDER, TARGET_FTP_FOLDER].join('/')

            remote_path = remote_path_string.split('/')
            remote_path.shift(1)
            remote_path.pop

            remote_path = convert_file_name(remote_path.join('/'), reverse:true)
         else
            remote_path_arr = remote_path_string.split('/')
            remote_path_arr.pop
            target_path = "/mnt/ftp/" + [ROOT_FTP_FOLDER, TARGET_FTP_FOLDER].join('/')

            remote_path = "#{remote_path_arr.join('/')}"
         end

         res = copy_field_passport_to_target(obj, remote_path, target_path, from_ftp: from_ftp)

         obj[:details_hash]["pdf_file_name"] = res[0][:file_name] #if lang.eql?(:rus)
         obj.save
      end
   end



   def self.copy_field_passport_to_target(obj, remote_path, target_path, from_ftp: nil)
      xls_path = XLS_PATH
      passport_result = []

      passport = obj[:details_hash]["passport_file"]

      if passport.present?
         check_ftp_connection
         res = passport.split('/')
         if res.is_a?(Array)
            passport = passport.split('/').last
         end

         if from_ftp
            file_on_ftp = file_exists_in_ftp(remote_path.split('/'), passport.split('/').last)

            if file_on_ftp[:result]
               local_file_path = [xls_path , convert_file_name(file_on_ftp[:passport])].join('/')


               # begin
               @ftp.getbinaryfile(["", remote_path.force_encoding("ASCII-8BIT"), file_on_ftp[:remote_file_path]].join('/'), local_file_path)
               # rescue
               #    byebug
               #    @ftp.getbinaryfile([remote_path.force_encoding("UTF-8"), file_on_ftp[:file_name]].join('/'), local_file_path)
               #
               # end

               ext = file_on_ftp[:passport].split('.').last.downcase.to_s
               dt = DateTime.now
               file_name = [dt.strftime("%Y%m%d%H%M%S%L"), ext].join('.')
               remote_new_file_path = [target_path, file_name].join('/')


               @ftp.putbinaryfile(local_file_path, remote_new_file_path)

               passport_result << {
                  file_name: file_name,
                  name: file_on_ftp[:passport] #convert_file_name(file_on_ftp[:file_name])
               }
            end
         else
            result_hash = file_exists_in_local(remote_path.split('/'), passport)

            passport_name = result_hash[:file_name].split('/').last

            if result_hash[:result]
               local_file_path = [xls_path , passport_name].join('/')
               FileUtils.cp(remote_path + '/' + passport, local_file_path)

               ext = passport_name.split('.').last.downcase.to_s
               dt = DateTime.now
               file_name = [dt.strftime("%Y%m%d%H%M%S%L"), ext].join('.')

               # remote_new_file_path = [target_path, file_name].join('/')
               # move_file(local_file_path, remote_new_file_path)

               truncated_file_name = truncate(file_name, 119)
               move_file(local_file_path, [target_path, truncated_file_name].join('/'))
               FileUtils.rm(local_file_path) if File.file?([target_path, truncated_file_name].join('/'))

               passport_result << {
                  file_name: file_name,
                  name: passport_name
               }
            end
         end
      end

      passport_result
   end


   # Отправляет запрос на сервис отчетов для генерации инвест. паспорта в формате pdf
   #
   # @param lang[Hash] - язык, на котором нужно сформировать инвест. паспорт
   # @param obj[Hash] obj - хэш с данными инвест. объекта
   # @param xls_path[String] - каталог, в котором располагаются pdf-файлы
   # @param from_ftp[Boolean] - флаг, работать ли с файлами по ftp или локально
   #
   def self.generate_pdf(lang, obj, xls_path, from_ftp: nil)
      ftp_path = generate_invest_passport_for_object(obj, lang, from_ftp: from_ftp)
      Dir.glob(xls_path + '/*.pdf').each { |file| File.delete(file)}
      @logger.info(ftp_path)
   end


   private


   # Создает json-файл с данными для отображения на карте. Файл копируется на ftp-сервер
   #
   # @param file_name[String] - имя файла, который будет создан
   # @param inv_objects[Hash] - хэш со списком инвестиционных объектов или объектов энергетики
   # @param from_ftp[Boolean] - режим работы с FTP (true) или локально (false)
   #
   def self.put_json_to_file(file_name, inv_objects, from_ftp: nil)
      xls_path = XLS_PATH
      file_name = [file_name, "txt"].join('.')
      worked_file = [xls_path, file_name].join('/')

      out_file = File.new(worked_file, "w")

      out_file.write(JSON.generate(inv_objects))
      out_file.close

      if from_ftp
         target_path = [ROOT_FTP_FOLDER, TARGET_FTP_FOLDER].join('/')
         check_ftp_connection
         begin
            @ftp.putbinaryfile(worked_file, [target_path, file_name].join('/'))
            Dir.glob(xls_path + '/*.txt').each { |file| File.delete(file)}
         rescue
            @logger.warn("Ошибка перемещения файла на FTP-сервер")
         end
      else
         target_path = "/mnt/ftp/#{ROOT_FTP_FOLDER}/#{TARGET_FTP_FOLDER}"

         move_file(worked_file, [target_path, file_name].join('/'))

         unless File.exist?([target_path, file_name].join('/'))
            @logger.warn("Ошибка копирования файла")
         end
      end
   end


   # Обход файлов инвест. объектов в локальном режиме
   #
   # @param testing[Boolean] - признак тестирования, размещает обработанные файлы в /imports/inventory-local
   # @param start_index[Integer] - индекс начального файла, для попадания в обработку (по-умолчанию -nil).
   # @param limit[Integer] - максимальное кол-во файлов, которые будут обработаны,
   #                             используется только для тестирования FTP (по-умолчанию -nil).
   # @param main_folder[String] - основной каталог поиска файлов
   # @param is_power[Boolean] - флаг, импортировать ли только объекты-мощности (true)
   # @param is_field[Boolean] - флаг, импортировать ли только объекты-месторождения (true)
   #
   # @return nil
   #
   def self.import_from_local(testing: false, start_index: nil, limit: nil, main_folder: nil, is_gas: false, is_power: false, is_field: false )

      xls_path = XLS_PATH
      logger_texts = LOGGER_TEXTS
      separator = LOG_SEPARATOR

      start_path = "/mnt/ftp/#{ROOT_FTP_FOLDER}" #FTP должен быть примонтирован к /mnt/ftp

      if testing

         start_path = "#{ROOT_PATH}/imports/inventory-local"
         check_path(start_path)
      else
         if is_gas

            start_path += "/#{GAS_FTP_FOLDER}"
         else

         if is_power

            start_path += "/#{POWER_FTP_FOLDER}"
         else
            if is_field

               start_path += "/#{FIELD_FTP_FOLDER}"
            else
               start_path += "/#{INVEST_FTP_FOLDER}"
            end
         end

         end
      end

      if main_folder.present?
         start_path += "/#{main_folder}"
      end

      files = []
      files.concat(find_files(start_path, limit: limit))

      @file_stat[:count] = files.size

      binding_progressbar = get_progressbar({is_ftp:false, total: files.count})

      files.each do |file_path|
         @errors = 0
         @error_list = []
         stopped = false

         ext = file_path.include?('.xlsx') ? 'xlsx' : 'xls'
         local_file_name = ['processing-invest-map', ext].join('.')

         current_handle_file = "обрабатывается файл: #{file_path}"
         binding_progressbar.title = "#{ap current_handle_file}"
         binding_progressbar.format = "%t |%B| [%f] %p%%  Обработано %c/%C файлов, успешно: #{@file_stat[:success]}"
         @logger.info([file_path, logger_texts[:work_started]].join(separator))
         total_time_start = Time.now

         # Удаляем предыдущие ошибки для обрабатываемого файла
         local_remove_prev_error_for_file(file_path)

         FileUtils.cp(file_path, xls_path + '/' + local_file_name)
         file_info = processing_invest_data(local_file_name, remote_file_path: file_path, from_ftp: false, testing: testing)

         past_time = Time.now - total_time_start
         @logger.info(["Обработка файла заняла", past_time, "секунд"].join(" "))

         file = File.basename(file_path)
         if testing
            folder = xls_path
         else
            folder = get_folder_from_path(file_path)
         end

         local_file_path = [xls_path , local_file_name].join('/')
         if @errors == 0

            file_name = file_path.split('/').last
            new_file_name = update_file_name(file_name, 'COMPLETE', ext)   #file_name.reverse.sub('.xls'.reverse, '_COMPLETE.xls'.reverse).reverse

            # move_file(local_file_path, new_file_name)

            truncated_file_name = truncate(new_file_name, 119)
            new_file_path = [folder , truncated_file_name].join('/')
            move_file(local_file_path, new_file_path)
            # FileUtils.rm(local_file_path) if File.file?(new_file_path)

            if File.file?(new_file_path)
               FileUtils.rm(file_path)

               @file_stat[:success] = @file_stat[:success] + 1
            end
         else

            file_name = file_path.split('/').last
            new_file_name = update_file_name(file_name, 'ERRORS', ext) #file_name.reverse.sub('.xls'.reverse, '_ERRORS.xls'.reverse).reverse

            truncated_file_name = truncate(new_file_name, 119)
            new_file_path = [folder , truncated_file_name].join('/')

            move_file(local_file_path, new_file_path)

            if File.file?(new_file_path)
               FileUtils.rm(file_path)

               puts_local_error_file(xls_path, testing: testing, folder: folder, file_name: file, is_power: is_power)
               @file_stat[:error] = @file_stat[:error] + 1
            end
         end

         if stopped
            @file_stat[:file_error] = @file_stat[:file_error] + 1
            puts_local_error_file(xls_path, testing: testing, folder: folder, file_name: file, is_power: is_power)
         end

         binding_progressbar.increment

         @logger.info(["Статистика: всего:", @file_stat[:count],
                       "пропущено:", @file_stat[:excluded],
                       "успешно:", @file_stat[:success],
                       "не прошли ФЛК:", @file_stat[:error],
                       "не обработано файлов::", @file_stat[:file_error]].join(" "))

         @logger.info(logger_texts[:footer])
      end
   end


   # Метод вставляет suffix в конец имени файла, заменяет расширение на ext, если ext задан
   # возвращает имя файла
   #
   # @param file_name[String] - имя файла.
   # @param suffix[String] - суффикс.
   # @param ext[Int] - расширение файла.
   #
   # @return [String] - имя файла
   #
   def self.update_file_name(file_name, suffix, ext = nil)
      new_file_name = file_name.reverse.sub('.xls'.reverse, "_#{suffix}.xls".reverse).reverse

      if ext.present?
         temp_file_name = new_file_name.split('.')
         temp_file_name.pop
         temp_file_name << ext
         new_file_name = temp_file_name.join('.')

      end
      new_file_name
   end


   # Метод устанавливает соединение с FTP-сервером и запускает процедуру
   # обхода папок
   #
   # @param start_index[Integer] - индекс начального файла, для попадания в обработку (по-умолчанию -nil).
   # @param limit[Integer] - максимальное кол-во файлов, которые будут обработаны (по-умолчанию -nil).
   # @param only_count[] -
   # @param is_power[Boolean] - флаг, импортировать ли только объекты-мощности (true)
   #
   # @return nil
   #
   def self.import_from_ftp(start_index: nil, limit: nil, only_count: nil, is_power: false)

      @ftp = ftp_connect
      @ftp.chdir(ROOT_FTP_FOLDER)

      if is_power
         @ftp.chdir(convert_file_name(POWER_FTP_FOLDER, reverse: true))
      else
         @ftp.chdir(convert_file_name(INVEST_FTP_FOLDER, reverse: true))
      end

      @file_stat = {
          :count => 0,
          :success => 0,
          :error => 0,
          :excluded => 0,
          :file_error => 0
      }
      working_files = []

      time_start = Time.now

      explore_ftp(working_files, start_index: start_index, limit: limit)
      past_time = Time.now - time_start

      @logger.info(["Найдено файлов для обработки:", working_files.count,
                    "Поиск занял:", past_time, "секунд"].join(" "))

      if !only_count.present? || !only_count
         binding_progressbar = get_progressbar({is_ftp:true, total:working_files.count})

         read_ftp_files(working_files, binding_progressbar, is_power: is_power)

         @logger.info(["Результат прохода:",
                       "обработато файлов:", @counts[:files],
                       "загружено объектов:", @counts[:objects]].join(" "))
      end
      @ftp.close
   end


   # Метод запускает процедуру обработки для всех файлов из массива working_files
   #
   # @param working_files[Array] - массив файлов {path, filename}
   # @param binding_progressbar[ProgressBar] - прогрессбар.
   # @param is_power[Boolean] - флаг, читать ли только объекты-месторождения (true)
   #
   # @return nil
   #
   def self.read_ftp_files(working_files, binding_progressbar, is_power: false)

      logger_texts = LOGGER_TEXTS
      separator = LOG_SEPARATOR

      working_files.each do |curr_file|
         file_name = convert_file_name(curr_file[:filename])
         file_path = [convert_file_name(curr_file[:path]), file_name].join("/")

         current_handle_file = "обрабатывается файл: #{file_path}"
         binding_progressbar.title = "#{ap current_handle_file}"
         binding_progressbar.format = "%t |%B| [%f] %p%%  Обработано %c/%C файлов, успешно: #{@file_stat[:success]}"

         @logger.info([file_path, logger_texts[:work_started]].join(separator))

         total_time_start = Time.now

         file_info = copy_and_read_file(curr_file[:path], curr_file[:filename], is_power: is_power)
         is_success = file_info.present? && file_info[:result]

         past_time = Time.now - total_time_start
         @logger.info(["Обработка файла заняла", past_time, "секунд"].join(" "))

         objects_count = file_info[:count]
         if objects_count.present?
            @counts[:objects] += objects_count if is_success

            @logger.info(["Результат обработки файла:",
                          "всего объектов:", objects_count,
                          "Результат:", file_info[:result]].join(" "))

            @logger.info ('Всего загружено объектов: %s' % @counts[:objects])
         else
            @logger.info("Результат неизвестен")
         end

         binding_progressbar.increment

         @logger.info(logger_texts[:footer])
      end

   end


   # Метод копирует файл с FTP сервера в локальную папку XLS_PATH, имя локального файла
   # задано в local_file_name, затем запускает метод обработки этого файла working_with_excel.
   # После обработки возвращает обработанный файл на FTP сервер и удаляет
   # исходный. Если невозможно прочитать файл (например если присутсвует "я" в имени файла)
   # кладет в папку с непрочитанным файлом info.txt с сообщением
   #
   # @param remote_path[String] - удаленный путь к файлу.
   # @param remote_file_name[String] - имя файла
   # @param is_power[Boolean] - флаг, читать ли только объекты-месторождения (true)
   #
   # @return [Hash] результат обработки файла
   #
   def self.copy_and_read_file(remote_path, remote_file_name, is_power: false)

      xls_path = XLS_PATH
      stopped = false # Признак остановки дальнейшей обработки текущего файла

      ext = remote_file_name.include?('.xlsx') ? 'xlsx' : 'xls'
      local_file_name = ['processing-invest-map', ext].join('.')

      remote_file_path = [remote_path, remote_file_name].join("/")
      file_info = {}

      @errors = 0
      @error_list = []

      remote_file_name_new = remote_file_name.sub('.xls', '_COMPLETE.xls')


      begin
         @ftp.getbinaryfile(remote_file_path, [xls_path , local_file_name].join('/'))
      rescue => error
         @logger.info(["Ошибка копирования файла с FTP:", convert_file_name(remote_file_path)].join(" "))
         stopped = true
      end

      unless stopped
         # begin
            file_info = processing_invest_data(local_file_name, remote_file_path: remote_file_path, from_ftp: true, is_power: is_power)
         # rescue Ole::Storage::FormatError
         #    @logger.info(["Неверный формат файла:", remote_file_path].join(" "))
         #    add_flk_error('', :error_format_file)
         #    stopped = true
         #
         # rescue Exception => ex
         #    if testing
         #       @logger.error(ex)
         #       puts ex
         #       # byebug
         #    end
         #    @logger.info(["Ошибка обработки файла:", remote_file_path].join(" "))
         #    add_flk_error('', :error_read_file)
         #    stopped = true
         # end
      end

      unless stopped
         worked_file = [xls_path, local_file_name].join('/')

         if @errors > 0
            #ext = File.extname(remote_file_name)
            remote_file_name_new = remote_file_name.sub('.xls', '_ERRORS.xls')
            @file_stat[:error] = @file_stat[:error] + 1
            dist_count = convert_file_name(remote_file_path).include?('БАШКОРТОСТАН') ? 3 : 2
            puts_error_file(@error_list, xls_path, remote_path, remote_file_name, dist_count)
         else
            @file_stat[:success] = @file_stat[:success] + 1
         end

         check_ftp_connection
         @ftp.putbinaryfile(worked_file, [remote_path, remote_file_name_new].join("/"))
         result = @ftp.last_response

         if result.include? '226'
            begin
               @ftp.delete(remote_file_path)
            rescue
               @logger.error(['Ошибка удаления файла', convert_file_name(remote_file_path)].join(' '))
            end
         else
            @logger.info('Ошибка передачи файла на FTP')
         end

         Dir.glob(xls_path + '/processing-invest-map.xls').each { |file| File.delete(file)}
         Dir.glob(xls_path + '/processing-invest-map.xlsx').each { |file| File.delete(file)}
      end

      file_info
   end


   # Метод считывает данные из заданного xls файла, проверяет данные по ФЛК,
   #  результат проверки записывается в файл processed.xls. Если проверка
   #  прошла успешно - добавляет данные в БД и запускает подсчет статистики по
   #  каждому объекту.
   #
   # @param file_name[String] - имя файла
   # @param remote_file_path[String] - удаленный путь к файлу
   # @param from_ftp[Boolean] - признак работы с FTP
   # @param testing[Boolean] - признак тестирования (по-умолчанию -false).
   # @param is_power[Boolean] - флаг, читать ли только объекты-месторождения (true)
   #
   # @return [Hash[count<Hash>, result[String], error_list[Array]]
   #
   def self.processing_invest_data(file_name, remote_file_path: nil, from_ftp: true, testing: false, is_power: false, is_gas: false)
      xls_path = XLS_PATH
      result = false
      contact_info_data_column_index = 2
      contact_info = {}
      @error_list = []

      file_path = [xls_path, file_name].join('/')

      xls = Roo::Spreadsheet.open(file_path)
      invest_object_list = []

      # Получаем массив (array) вкладок документа xls
      sheets = xls.sheets

      type_fields_list = []
      type_fields_sheet_num = sheets.index(TYPE_FIELD_REF)

      if type_fields_sheet_num.present?
         obj_list = xls.sheet(type_fields_sheet_num).parse

         obj_list.each do |obj|
            res = {
               id: obj[0],
               rus_name: obj[1],
               eng_name: obj[2],
            }

            type_fields_list << res
         end
      end

      addr_sheet_num = sheets.index(CONTACTS_INFO)

      if addr_sheet_num.present?
         xls.each_with_pagename do |page_name, page_data|
            if page_name.eql?(CONTACTS_INFO)
               contact_info = InvestArea.prepare_contact_info(page_data.column(contact_info_data_column_index))

               if contact_info[:type].eql?(OBJECTS_AREA_TYPE)
                  # if contact_info[:version] < OBJECTS_MINIMAL_FILE_VERSION
                  #    add_flk_error('', :object_file_version_err, contact_info[:version])
                  #    return  {
                  #               count: 0,
                  #               result: false
                  #            }
                  # end

                  check_email_not_empty(contact_info, OBJECTS_AREA_TYPE)
                  check_phones_not_empty(contact_info, OBJECTS_AREA_TYPE)
               end

               # if (contact_info[:type].eql?(POWER_AREA_TYPE) && contact_info[:version] < POWER_MINIMAL_FILE_VERSION)
               #    add_flk_error('', :power_file_version_err, contact_info[:version])
               #    return  {
               #       count: 0,
               #       result: false
               #    }
               # end
               #
               # if (contact_info[:type].eql?(FIELD_AREA_TYPE) && contact_info[:version] < FIELD_MINIMAL_FILE_VERSION)
               #    add_flk_error('', :field_file_version_err, contact_info[:version])
               #    return  {
               #       count: 0,
               #       result: false
               #    }
               # end

               if contact_info[:type].eql?(POWER_AREA_TYPE) || contact_info[:type].eql?(GAS_AREA_TYPE)
                  current_type = contact_info[:type]
                  check_company_name_not_empty(contact_info)
                  #check_email_not_empty(contact_info, POWER_AREA_TYPE)
                  check_email_not_empty(contact_info, current_type)
                  #check_phones_not_empty(contact_info, POWER_AREA_TYPE)
                  check_phones_not_empty(contact_info, current_type)
               end

               prepare_site_field(contact_info)
            elsif page_name.eql?(CAPACITY)
               row_count = 0
               page_data.each do |row|
                  row_count += 1
                  if row_count > 1

                     #object_structure = InvestArea.prepare_object_power_data_from_array(row, convert_file_name(remote_file_path))
                     object_structure = InvestArea.prepare_object_power_data_from_array(row, convert_file_name(remote_file_path),contact_info[:type])
                     obj = object_structure.merge(contact_info)

                     invest_object_list << obj

                     check_area_name_not_empty(obj)
                     check_coordinates(obj, POWER_AREA_TYPE)
                     unless check_correct_coordinates(obj, POWER_AREA_TYPE)
                        obj[:details_hash][:cord_x] = nil
                        obj[:details_hash][:cord_y] = nil
                     end

                     obj[:details_hash][:contract_power] = NO_DATA_RUS unless check_power_value_correct(obj[:details_hash][:contract_power])
                     obj[:details_hash][:metered_power] = NO_DATA_RUS unless check_power_value_correct(obj[:details_hash][:metered_power])
                     obj[:details_hash][:reserve_power] = NO_DATA_RUS unless check_power_value_correct(obj[:details_hash][:reserve_power])
                     obj[:details_hash][:request_count] = NO_DATA_RUS unless check_power_value_correct(obj[:details_hash][:request_count])
                     obj[:details_hash][:rebuild_time] = NO_DATA_RUS unless obj[:details_hash][:rebuild_time].present?
                  end
               end
            elsif page_name.eql?(FIELD_INFO)
               row_count = 0
               page_data.each do |row|
                  row_count += 1
                  if row_count > 4

                     if from_ftp
                        object_structure = InvestArea.prepare_object_field_data_from_array(row, convert_file_name(remote_file_path), type_fields_list)
                     else
                        object_structure = InvestArea.prepare_object_field_data_from_array(row, remote_file_path, type_fields_list)
                     end

                     obj = object_structure.merge(contact_info)

                     obj[:details_hash]["address_string"] = get_object_info(obj)

                     invest_object_list << obj

                     check_field_name_not_empty(obj)
                     check_field_name_eng_not_empty(obj)
                     check_use_not_empty(obj)
                     # check_license_not_empty(obj)
                     # check_license_eng_not_empty(obj)
                     check_type_unit_not_empty(obj)
                     check_coordinates(obj, FIELD_AREA_TYPE)
                     check_correct_coordinates(obj, FIELD_AREA_TYPE)

                     check_field_pdf_file_exist(obj, remote_file_path, from_ftp)
                  end
               end
            elsif page_name.eql?(OBJECTS_INFO)
               row_count = 0

               page_data.each do |row|
                  row_count += 1

                  if row_count > 3 && row[0].present?

                     object_structure =
                           if from_ftp
                              InvestArea.prepare_object_data_from_array(row, convert_file_name(remote_file_path))
                           else
                              path = remote_file_path.split('/')
                              path.shift(3)
                              path = ["", path].join('/')

                              InvestArea.prepare_object_data_from_array(row, path)
                           end

                     obj = object_structure.merge(contact_info)
                     invest_object_list << obj


                     check_area_name_not_empty(obj)


                     check_coordinates(obj, OBJECTS_AREA_TYPE)
                     check_correct_coordinates(obj, OBJECTS_AREA_TYPE)
                     check_photos_complete(obj)

                     check_cadastre_number_field(obj)

                     check_not_uniq_object(obj, invest_object_list)

                  #   check_invest_area_type_not_empty(obj)

                     check_type_in_list(obj,suggestion_for_using: true) # if check_offer_for_using_exist(obj)
                     check_type_in_list(obj,type: true) # if check_area_type_not_empty(obj)
                     check_type_in_list(obj,area_condition: true) # if  check_area_condition_not_empty(obj)

                     prepare_fields_by_default(OBJECTS_AREA_TYPE, obj)

                     if check_cyrillic_symbols_error(obj[:details_hash][:area_name_eng])
                        add_flk_error(obj[:details_hash][:area_name_eng], :area_name_eng_invest_cyrillic_error, obj[:details_hash][:area_name_eng])
                     end

                     if check_cyrillic_symbols_error(obj[:details_hash][:description_eng])
                        add_flk_error(obj[:details_hash][:description_eng], :description_eng_invest_cyrillic_error, obj[:details_hash][:description_eng])
                     end

                     check_objects_photo_files_exists(obj, remote_file_path, from_ftp)
                  end
               end
            end
         end

         address_hash = InvestArea.prepare_object_address_data(xls) if contact_info[:type].eql?(OBJECTS_AREA_TYPE)

         if address_hash.present?
            check_region_not_empty(address_hash)
            check_village_council_not_empty(address_hash)
            check_locality_not_empty(address_hash)
         end

         if invest_object_list.count > 0 && contact_info.present?

            result_list = []
            invest_object_list.each do |obj|
               if contact_info[:type].eql?(OBJECTS_AREA_TYPE)
                  obj2 = obj.merge(address_hash)
               else
                  obj2 = obj
               end

               obj2.delete(:type)
               obj2.delete(:version)
               result_list << obj2
            end
            invest_object_list = result_list
         end

         dist_count = convert_file_name(remote_file_path).include?('БАШКОРТОСТАН') ? 3 : 2

         if @error_list.size == 0
            if invest_object_list.count > 0
               result = save_objects(invest_object_list)
            end

         else
            invest_object_list = []
            result = false

            folders = remote_file_path.split('/')
            remote_file_name = folders.pop
            remote_path = folders.join('/')

            if from_ftp
               puts_error_file(@error_list, xls_path, remote_path, remote_file_name, dist_count)
            else
               puts_local_error_file(xls_path, testing: testing, folder: folders, file_name: remote_file_name, is_power: is_power)
            end
         end
         objects_count = invest_object_list.count
      else
         add_flk_error('', :contacts_sheet_not_found)
      end

      {
         count: objects_count,
         result: result
      }
   end


   # Проверка поля "Месторождение" на заполненность
   #
   # @param obj[InvestMap] - объект месторождения
   #
   def self.check_field_name_not_empty(obj)
      unless obj[:area_name].present?
         add_flk_error(obj[:area_name], :field_name_is_empty, obj[:area_name])
      end
   end




   # Проверка поля "Месторождение (на англ. яз)" на заполненность
   #
   # @param obj[InvestMap] - объект месторождения
   #
   def self.check_field_name_eng_not_empty(obj)
      unless obj[:details_hash][:area_name_eng].present?
         add_flk_error(obj[:details_hash][:area_name_eng], :field_name_eng_is_empty, obj[:details_hash][:area_name_eng])
      end
   end


   # Проверка поля "Полезное ископаемое. применение" на заполненность
   #
   # @param obj[InvestMap] - объект месторождения
   #
   def self.check_use_not_empty(obj)
      unless obj[:details_hash][:use].present?
         add_flk_error(obj[:details_hash][:use], :use_is_empty, obj[:details_hash][:use_eng])
      end
   end


   # Проверка поля "Лицензия" на заполненность
   #
   # @param obj[InvestMap] - объект месторождения
   #
   def self.check_license_not_empty(obj)
      unless obj[:details_hash][:license].present?
         add_flk_error(obj[:details_hash][:license], :license_is_empty, obj[:details_hash][:license])
      end
   end



   # Проверка поля "Лицензия (на англ. яз)" на заполненность
   #
   # @param obj[InvestMap] - объект месторождения
   #
   def self.check_license_eng_not_empty(obj)
      unless obj[:details_hash][:license_eng].present?
         add_flk_error(obj[:details_hash][:license_eng], :license_eng_is_empty, obj[:details_hash][:license_eng])
      end
   end



   # Проверка поля "Единица измерения" на заполненность
   #
   # @param obj[InvestMap] - объект месторождения
   #
   def self.check_type_unit_not_empty(obj)
      unless obj[:details_hash][:type_unit].present?
         add_flk_error(obj[:details_hash][:type_unit], :type_unit_is_empty, obj[:details_hash][:type_unit])
      end
   end



   # Проверяет существование файлов по указанному пути
   #
   # @param obj[InvestMap] - инвест. объект
   # @param remote_file_path[String] - путь к файлу
   # @param from_ftp[Boolean] - признак работы с FTP
   #
   def self.check_objects_photo_files_exists(obj, remote_file_path, from_ftp)
      if from_ftp && remote_file_path.present?
         result = check_photo_file_exists(obj[:details_hash][:photos], remote_file_path)

      elsif !from_ftp && remote_file_path.present?
         result = check_local_photo_file_exists(obj[:details_hash][:photos], remote_file_path)
      else
         logger.warn("Невозможно проверить существование файлов фото. Механизм не реализован")
         puts "Невозможно проверить существование файлов фото. Механизм не реализован"
      end

      if result[:errors].count > 0
         result[:errors].each do |res|
            add_flk_error(res[:photo_name],
                          :photo_file_not_found,
                          res[:photo_name])
         end
      end
   end


   # Проверка поля "Паспорт объекта" на заполненность
   #
   # @param obj[InvestMap] - объект месторождения
   #
   def self.check_field_pdf_file_exist(obj, remote_file_path, from_ftp)
      result = check_field_pdf_file_exists(obj[:details_hash][:passport_file], remote_file_path)

      if result[:errors].count > 0
         result[:errors].each do |res|
            add_flk_error(res[:file_name], :field_passport_not_found, res[:file_name])
         end
      end
   end


   # Проверка полей "Фото..." на заполненность
   #
   # @param obj[InvestMap] - объект месторождения
   #
   def self.check_photos_complete(obj)
      obj[:details_hash][:photos].each do |photo|
         unless photo[:name].present?
            add_flk_error(obj[:area_name], :photos_not_complete)
            break
         end
      end
   end


   # @example ImportPropertyObject.get_error_statistic
   #
   # Метод обрабатывает все файлы ошибок на FTP сервере и собирает
   # статистику ошибок по кодам ошибок. Результат выводит в консоль и записывает лог.
   #
   # @return
   #
   def self.get_error_statistic
      logger_texts = LOGGER_TEXTS
      @logger.info(logger_texts[:footer])
      @logger.info(logger_texts[:start_error_statistic])

      @error_list = []
      @ftp = ftp_connect
      @ftp.chdir(ROOT_FTP_FOLDER)

      explore_ftp_errors(method: 'read_error_file')

      @logger.info("СТАТИСТИКА ОШИБОК:")
      @logger.info(@error_list.sort_by {|obj| -obj[:count]})

      puts 'Статистика ошибок'
      puts ap @error_list.sort_by {|obj| -obj[:count]}
      @ftp.close
   end

   #TODO: ???
   def self.read_error_file(raw_file_name)
      puts convert_file_name(raw_file_name)
      xls_path = XLS_PATH
      local_file_name = 'error-check.xlsx'
      stopped = false # Признак остановки дальнейшей обработки текущего файла

      remote_file_path = [@ftp.pwd, raw_file_name].join("/")
      begin
         @ftp.getbinaryfile(remote_file_path, [xls_path , local_file_name].join('/'))
      rescue => error
         @logger.error(["Ошибка копирования файла с FTP:", convert_file_name(remote_file_path)].join(" "))
         stopped = true
      end

      unless stopped
         begin
            processing_error_data(local_file_name)
         rescue
            @logger.error(["Ошибка обработки файла:", convert_file_name(remote_file_path)].join(" "))
         end

         Dir.glob(xls_path + '/error-check.xls').each { |file| File.delete(file)}

      end

      check_ftp_connection
   end


   # Метод проверяет существование файлов указанных в полях foto_1_name и foto_2_name
   # на FTP-сервере в каталоге с файлом, если файл отсутствует добавляет ошибку отсутствия файла
   #
   # @param photos[Array] - список фотографий
   # @param remote_file_path[String] - удаленный путь к файлу
   #
   # @return [Hash] - errors пустой, если нет ошибок
   #
   def self.check_photo_file_exists(photos, remote_file_path)
      result = {
         errors: []
      }
      if photos.size > 0 && remote_file_path.present?
         photos.each do |photo|
            files = []
            files << photo[:name] if photo[:name].present?

            if files.size > 0
               path = remote_file_path.split('/')
               path.shift(2)
               path.pop

               check_ftp_connection

               files.each do |file_name|
                  # begin
                     if file_exists_in_ftp(path, file_name)[:result]
                        photo_file_path = "#{convert_file_name(path.join('/'))}/#{file_name}"

                        if file_name.eql?(photo[:name])
                           photo[:name] = photo_file_path
                        else
                           @logger.warn("Для файла '#{file_name}' не найдено соответствий")
                        end
                     else
                        result[:errors].concat([{
                                                   photo_name: photo[:name],
                                                   file_name: file_name
                                                }])
                     end
                  # rescue
                  #    byebug
                  #    @logger.warn("ОШИБКА")
                  # end
               end


            end
         end
      end
      result
   end


   # Метод проверяет существование файлов указанных в полях foto_1_name и foto_2_name
   # на FTP-сервере в каталоге с файлом, если файл отсутствует добавляет ошибку отсутствия файла
   #
   # @param photos[Array] - список объектов
   # @param remote_file_path[String] - удаленный путь к файлу в сыром виде
   #
   # @return [Hash] - errors пустой, если нет ошибок
   #
   def self.check_local_photo_file_exists(photos, remote_file_path)

      result = {
         errors: []
      }
      if photos.size > 0 && remote_file_path.present?
         photos.each do |photo|
            files = []
            files << photo[:name] if photo[:name].present?

            if files.size > 0
               path = remote_file_path.split('/')
               # path.shift(2)
               path.pop

               files.each do |file_name|
                  # begin
                  result_hash = file_exists_in_local(path, file_name)

                  if result_hash[:result]
                     photo_file_path = "#{path.join('/')}/#{File.basename(file_name)}"

                     path = photo_file_path.split('/')
                     path.shift(3)
                     path = ["", path].join('/')

                     if file_name.eql?(photo[:name])
                        photo[:name] = path
                        photo[:remote_file_name] = result_hash[:file_name]
                     else
                        @logger.warn("Для файла '#{file_name}' не найдено соответствий")
                     end
                  else
                     result[:errors].concat([{
                                                photo_name: photo[:name],
                                                file_name: file_name
                                             }])
                  end
                  # rescue
                  #    byebug
                  #    @logger.warn("ОШИБКА")
                  # end
               end
            end
         end
      end
      result
   end



   # Метод проверяет существование файлов (сформированные паспорта объектов в формате pdf)
   # на FTP-сервере в каталоге с файлом, если файл отсутствует добавляет ошибку отсутствия файла
   #
   # @param file_name[String] - имя файла
   # @param remote_file_path[String] - удаленный путь к файлу в сыром виде
   #
   # @return [Hash] - errors пустой, если нет ошибок
   #
   def self.check_field_pdf_file_exists(file_name, remote_file_path)

      result = {
         errors: []
      }

      path = remote_file_path.split('/')
      path.pop

      if file_exists_in_local(path, file_name)[:result]
         pdf_file_path = "#{path.join('/')}/#{File.basename(file_name)}"

         path = pdf_file_path.split('/')
         path.shift(3)
         path = ["", path].join('/')
      else
         result[:errors].concat([{
                                    file_name: file_name
                                 }])
      end

      result
   end


   # Метод читает файлы ошибок и формирует список ошибок @error_list
   #
   # @param [String] file_name - путь к конечному каталогу
   #
   def self.processing_error_data(file_name)
      options = Hash[
         :code_error, 'Код ошибки'
      ]
      xls_path = XLS_PATH

      file_path = [xls_path , file_name].join('/')

      book = Roo::Spreadsheet.open(file_path)

      row_count = 0

      book.each_with_pagename do |page_name, page_data|

         page_data.header_line = 2
         code_column_num = page_data.row(page_data.header_line).index(options[:code_error]) + 1

         codes = page_data.column(code_column_num)

         codes.each do |code|
            row_count += 1

            if row_count > 2
               insert_error_in_error_list(code.to_i)
            end

         end

      end
   end


   # Метод вставляет запись или увеличивает счетчик у найденной записи по коду
   # ошибки в списке ошибок @error_list, если задан error_code то список
   # собирается только по данной ошибке и в files собираются пути к файлам с данной ошибкой
   #
   # @param code[String] - путь к конечному каталогу
   #
   def self.insert_error_in_error_list(code)

      if code.present?
         if @error_list.count == 0
            @error_list << {
               code: code,
               description: get_error_description(code),
               count: 1
            }
         else
            finded = false
            @error_list.each do |error|
               if error[:code] == code
                  finded = true
                  error[:count] += 1
               end
            end

            unless finded
               @error_list << {
                  code: code,
                  description: get_error_description(code),
                  count: 1
               }
            end
         end
      end
   end


   # Возвращает описание ошибки по ее коду
   #
   # @param [String] code - код ошибки
   #
   def self.get_error_description(code)
      error_reference = ERROR_REFERENCE

      error_reference.each do |key, value|

         if value[:id] == code
            return value[:description]
         end
      end
      ''
   end


   # Создает новый xls-файл и записывает в него список найденных ошибок
   #
   # @param error_list[Array] - массив ошибок
   # @param file_name[String] - имя файла
   # @param remote_file_name[String] - удаленное имя файла в понятном виде
   #
   # @return nil
   #
   def self.write_errors_to_file(error_list, file_name, remote_file_name)
      workbook = WriteXLSX.new([XLS_PATH, file_name].join('/'))

      err_sheet = workbook.add_worksheet(ERROR_SHEET_NAME)

      err_sheet.write_row('A1', [remote_file_name])
      err_sheet.write_row('A2', ['Код ошибки', 'Описание ошибки', 'Наименование объекта', 'Поле', 'Значение'])


      row_count = 3

      error_list.each do |error_row|
         err_sheet.write_row('A' + row_count.to_s, [error_row[:error_id], error_row[:description], error_row[:name_object], error_row[:field], error_row[:value]])

         row_count += 1
      end

      workbook.close
   end


   # Если поле "Сайт" не заполенно, то заполняется значением "не предоставлено
   #        поставщиком данных" на рус. языке
   #
   # @param contact_info[Hash] - Хэш с контактными данными
   #
   def self.prepare_site_field(contact_info)
      unless contact_info[:contacts_hash][:site].present?
         contact_info[:contacts_hash][:site] = NO_DATA_RUS
      end
   end



   # Проверка поля на заполненность "Наименование объекта, краткая характеристика на русском"
   #
   # @param obj[InvestMap] - объект инвентаризации
   #
   # return
   def self.check_invest_area_type_not_empty(obj)
      unless obj[:details_hash][:invest_area_type].present?
         add_flk_error(obj[:details_hash][:invest_area_type], :invest_area_type_is_empty, obj[:details_hash][:invest_area_type])
      end
   end


   # Проверка объекта на уникальность. В базе данных не должно быть объектов с
   #     идентичным кадастровым номером
   #
   # @param obj[InvestMap] - объект инвентаризации
   # @param invest_object_list [Array] - список инвест. объектов
   #
   # return
   def self.check_not_uniq_object(obj, invest_object_list)
      is_error = false

      if obj[:cadastre_number].present?
         obj_count = 0
         invest_object_list.each do |invest_object|
            if invest_object[:cadastre_number].eql?(obj[:cadastre_number])
               obj_count += 1
            end
         end

         if obj_count >= 2
            add_flk_error(obj[:cadastre_number], :not_uniq_object_in_list_err, obj[:cadastre_number])
            is_error = true
         end

         unless is_error
            invest_area = InvestArea.where('cadastre_number = ?', obj[:cadastre_number].to_s).first

            if invest_area.present?
               add_flk_error(obj[:cadastre_number], :not_uniq_object_err, obj[:cadastre_number])
            end
         end
      end
   end


   # Проверка поля Кадастровый номер
   #
   # @param obj[InvestMap] - инвест-объект
   #
   # @return
   def self.check_cadastre_number_field(obj)
      if obj[:cadastre_number].present?
         cadastre_number = obj[:cadastre_number].to_s
         begin
            res = /\d{2}:\d{2}:\d{6,7}:\d{,8}/.match cadastre_number
            if (!res.present?)
               add_flk_error(cadastre_number, :cadastre_number_err, cadastre_number)
            end
         rescue
            add_flk_error(cadastre_number, :cadastre_number_err, cadastre_number)
             byebug
         end
      else
         add_flk_error(obj[:cadastre_number].to_s, :cadastre_number_err, obj[:cadastre_number].to_s)
      end
   end


   # Проверка поля "Тип площадки" на заполненность
   #
   # @param obj[InvestMap] - объект инвентаризации
   #
   # return
   def self.check_area_name_not_empty(obj)
      unless obj[:area_name].present?
         if obj[:area_type] == 1
            add_flk_error(obj[:area_name], :area_name_empty, obj[:area_name])
         else
            add_flk_error(obj[:area_name], :area_name_power_empty, obj[:area_name])
         end
      end
   end

   # Проверка поля "Тип площадки" на заполненность TODO: После успешного тестирования удалить
   #
   # @param obj[InvestArea] - инвест объект
   #
   def self.check_area_type_not_empty(obj)

      object_type = obj[:details_hash][:invest_area_type]

      if AREA_TYPES[:"#{object_type}"].nil?
         add_flk_error(object_type, :suggestion_for_using_dont_correct, object_type)
         return false
      end
      return true

   end

   def self.check_offer_for_using_exist(obj)
      offer_for_using  = obj[:details_hash][:use_types]

      if  OFFERS_FOR_USING[:"#{offer_for_using}"].nil?
         add_flk_error(offer_for_using, :suggestion_for_using_dont_correct, offer_for_using)
         return false
      end
      return true
   end


   def self.check_area_condition_not_empty(obj)

      current_state =  obj[:details_hash][:current_state]

      if AREA_CONDITIONS[:"#{current_state}"].nil?
         add_flk_error(current_state, :suggestion_for_using_dont_correct, current_state)
         return false
      end
      return true
   end


   # Проверка поля "Тип площадки". Значение поля должно совпадать со значением из справочника
   # В противном случае формируется ошибка
   #
   # @param obj[InvestMap] - объект инвентаризации
   #
   def self.check_type_in_list(obj,type: false, area_condition: false, suggestion_for_using: false)

      checking_field = obj[:details_hash][:invest_area_type] if type
      checking_field = obj[:details_hash][:current_state] if area_condition
      checking_field = obj[:details_hash][:use_types] if suggestion_for_using

      if obj[:area_type] == 1 && checking_field.present?
         unless check_value_in_type_area_list(checking_field,type,area_condition,suggestion_for_using)
            add_flk_error(checking_field, :invest_type_area_invalid_value, checking_field) if type
            add_flk_error(checking_field, :condition_dont_correct, checking_field) if area_condition
            add_flk_error(checking_field, :suggestion_for_using_dont_correct, checking_field) if suggestion_for_using
         end
      end

   end



   # Проверка наличия значения в справочнике "Тип площадки"
   #
   # @param type_area[String] - тип площадки
   #
   # @return [Boolean]
   #
   def self.check_value_in_type_area_list(type_area,type,condition,suggestion_for_using)
      type_area_list = TYPE_AREA_LIST if type
      type_area_list = CURRENT_OBJECT_STATE_LIST if condition
      type_area_list = USING_SITE_SUGGESTION_LIST if suggestion_for_using
      type_area_list.each do |tarea|
         if (tarea[:name].eql?(type_area) || tarea[:name].eql?(Unicode::capitalize(type_area)))
            return true
         end
      end

      return false
   end


   # Проверка на заполненность поля "Электронная почта". Если поле не заполнено - формируется ошибка
   #
   # @param contact_info[Hash] - Хэш с контактными данными
   # @param type_invest[String] - Тип площадки
   #
   def self.check_email_not_empty(contact_info, type_invest)
      unless contact_info[:contacts_hash][:email].present?
         if type_invest.eql?(OBJECTS_AREA_TYPE)
            message = :email_invest_is_empty
         else
            message = :email_power_is_empty
         end
         add_flk_error(contact_info[:contacts_hash][:email], message, contact_info[:contacts_hash][:email])
      end
   end


   # Проверка поля "Наименование организации" на заполненность
   #
   # @param contact_info[Hash] - Хэш с контактными данными
   #
   # return
   def self.check_company_name_not_empty(contact_info)
      unless contact_info[:contacts_hash][:company_name].present?
         add_flk_error(contact_info[:contacts_hash][:company_name], :company_name_is_empty, contact_info[:contacts_hash][:company_name])
      end
   end

   # Проверка поля на заполненность "Номер района"
   #
   # @param address_hash[Hash] - Хэш с адресными данными
   #
   # return
   def self.check_region_not_empty(address_hash)
      unless address_hash[:address_hash][:region][:ao_guid].present?
         add_flk_error(address_hash[:address_hash][:region][:ao_guid], :region_is_empty, address_hash[:address_hash][:region][:ao_guid])
      end
   end


   # Проверка поля на заполненность "Номер сельсовета"
   #
   # @param address_hash[Hash] - Хэш с адресными данными
   #
   # return
   def self.check_village_council_not_empty(address_hash)
      unless address_hash[:address_hash][:village_council][:ao_guid].present?
         add_flk_error(address_hash[:address_hash][:village_council][:ao_guid], :village_council_is_empty, address_hash[:address_hash][:village_council][:ao_guid])
      end
   end


   # Проверка поля на заполненность "Номер населенного пункта"
   #
   # @param address_hash[Hash] - Хэш с адресными данными
   #
   # return
   def self.check_locality_not_empty(address_hash)
      unless address_hash[:address_hash][:locality][:ao_guid].present?
         add_flk_error(address_hash[:address_hash][:locality][:ao_guid], :locality_is_empty, address_hash[:address_hash][:locality][:ao_guid])
      end
   end


   # Проверка значения мощности на число
   #
   # @param power_value[String] - значение мощности
   #
   # @return Boolean - результат проверки
   #
   def self.check_power_value_correct(power_value)
      if power_value.present?
         result = /\d+(\.\d+)/.match power_value.to_s

         return result.present?
      end

      false
   end


   # Проверка - поле не должно содержать символы кириллицы
   #
   # @param eng_description[String] - значение поля на англ. языке
   #
   # @return [Boolean] - результат проверки
   #
   def self.check_cyrillic_symbols_error(eng_description)
      arr = eng_description.scan /\p{Cyrillic}/

      if arr.length > 0
         return true
      else
         return false
      end
   end



   # Проверка поля на заполненность "Телефонные номера для связи"
   #
   # @param contact_info[Hash] - Хэш с контактными данными
   # @param type_area[String] - Тип инвест. объекта
   #
   # return
   def self.check_phones_not_empty(contact_info, type_area)
      phones = contact_info[:contacts_hash][:phones]

      if phones.present?
         phones.each do |phone|
            if phone[:phone_num].present?
               return
            end
         end
      end

      if type_area.eql?(OBJECTS_AREA_TYPE)
         add_flk_error(nil, :phone_invest_is_empty, nil)
      else
         add_flk_error(nil, :phone_power_is_empty, nil)
      end
   end



   # Проверяет входят ли указанные координатные точки в Башкортостан
   #
   # @param obj[Hash] - хэш с данными инвест. объекта
   # @param type_area[String] - тип площадки
   #
   # @param [Boolean]
   #
   def self.check_correct_coordinates(obj, type_area)
      if obj[:details_hash].present? && obj[:details_hash][:cord_x].present?  && obj[:details_hash][:cord_y].present?

         longitude = obj[:details_hash][:cord_y].strip
         latitude = obj[:details_hash][:cord_x].strip

         polygon = get_polygon_for_coordinates(longitude, latitude, type_area, obj)

         unless polygon.present?
            if type_area.eql?(OBJECTS_AREA_TYPE)
               message = :cord_invest_out_of_bounds

            elsif type_area.eql?(POWER_AREA_TYPE)
               message = :cord_power_out_of_bounds

            else
               message = :cord_field_out_of_bounds
            end

            add_flk_error(obj[:area_name], message)
            return false
         end

         return true
      end

      false
   end


   # Проверка заполнености полей. Если поле не заполнено, то в него автоматически
   # подставляется значение из константы NO_DATA_RUS
   #
   # @param type_object[String] - тип объекта
   # @param obj[Hash] - инвест. объект
   #
   def self.prepare_fields_by_default(type_object, obj)
      if type_object.eql?(POWER_AREA_TYPE)
         checked_fields_arr = []
      elsif type_object.eql?(OBJECTS_AREA_TYPE)
         checked_fields_arr = [:safe_area, :current_state, :address, :total_land_footage, #:cadastre_number,
                                                    :total_oks_footage, :ownership_type, :attraction_terms, :preliminary_cost,
                                                    :permitted_use, :auto_distance, :rails_distance, :gas_description,
                                                    :gas_available, :heating_description, :heating_available, :electric_description,
                                                    :electric_available, :water_description, :water_available, :sewerage_description,
                                                    :sewerage_available, :treatment_facilities_description, :treatment_facilities_available,
                                                    :use_types, :description, :description_rus]

         obj[:details_hash][:cadastre_number] = NO_DATA_RUS unless obj[:details_hash][:cadastre_number].present?

         obj[:details_hash][:area_name_eng] = NO_DATA_ENG unless obj[:details_hash][:area_name_eng].present?
         obj[:details_hash][:description_eng] = NO_DATA_ENG unless obj[:details_hash][:description_eng].present?
      end

      checked_fields_arr.each do |field_name|
         unless obj[:details_hash][field_name].present?
            obj[:details_hash][field_name] = NO_DATA_RUS
         end
      end
   end



   # Проверяет входят ли указанные координатные точки в Башкортостан
   #
   # @param longitude[String] - Координата Х
   # @param latitude[String] - Координата У
   # @param type_area[String] - тип площадки
   # @param obj[Hash] - хэш с данными инвест. объекта
   #
   def self.get_polygon_for_coordinates(longitude, latitude, type_area, obj)
      #сначала идет Y потом X т.к. синтаксис ST_Point(float x_lon, float y_lat)
      begin
         polygons =
            GisBoundaryPolygon.where("ST_Contains(geom
                                          , ST_GeometryFromText('POINT(#{longitude} #{latitude})', 4326)
                                         )
                                        ")
         polygons.each do |pol|
            if pol[:admin_lvl].eql?('4')
               return pol
            end
         end
      rescue
         if type_area.eql?(OBJECTS_AREA_TYPE)
            add_flk_error(obj[:area_name], :cord_invest_invalid_value)
         else
            add_flk_error(obj[:area_name], :cord_power_invalid_value)
         end
      end
   end


   # Определение наименования района в который входит объект по его координатам
   #
   # @param obj[InvestArea] - объект
   #
   # return [String, nil]
   def self.get_polygon_info(obj)

      details = obj[:details_hash]
      lon = details["cord_y"]
      lat = details["cord_x"]
      begin
         polygons =
             GisBoundaryPolygon.where("ST_Contains(geom
                                          , ST_GeometryFromText('POINT(#{lon} #{lat})', 4326)
                                         )
                                        ")
         polygons.each do |pol|
            if pol[:admin_lvl].to_i == 6
               return pol["name"]
               # return pol["name"].split(" ").first
            end
         end
      end
      nil
   end



   # Возвращает адрес объекта (строкой) по его координатам. Вызывает метод send_https_request,
   #  который отправляет запрос к сервису nominatim.openstreetmap.org
   #
   # @param obj[InvestArea] - объект
   #
   def self.get_object_info(obj)
      # obj = InvestArea.find(24)
      details = obj[:details_hash]
      lon = details[:cord_y]
      lat = details[:cord_x]

      answer = send_https_request(lat, lon)

      if answer.present?

         r = answer.readable
         if r.first[:result_string].present?
            res_str = r.first[:result_string]
            x =/.*<result\b[^>]*>(.*)<\/result>/.match(res_str)
            begin

            return x[1]
               rescue byebug
            end
         end
      end
   end


   # Отправляет запрос к сервису nominatim.openstreetmap.org. По переданным координатам (X, Y)
   # получает адрес в текстовом виде в формате xml
   #
   # @param lat[Float] - широта
   # @param lon[Float] - долгота
   #
   # @return [XML] Ответ от сервиса
   #
   def self.send_https_request(lat, lon)
      uri_path =
          'https://nominatim.openstreetmap.org/reverse'
      params =
          {:format => 'xml', :lat => lat, :lon => lon, :zoom => 18, :addressdetails => 1}

      uri_string =
              uri_path +
              "?" +
              params.map{|k,v| "#{k}=#{CGI::escape(v.to_s)}"}.join('&')
      uri = URI(uri_string)

      Net::HTTP.start(uri.host,
                      uri.port,
                      :use_ssl => uri.scheme == 'https',
                      :verify_mode => OpenSSL::SSL::VERIFY_NONE) do |http|
         request = Net::HTTP::Get.new uri.request_uri
         response = http.request request
         # puts response.body
         response.body
      end
   end


   # Формирует текст ФЛК ошибки
   #
   # @param name_object[String] - имя инвест. объекта, при проверке данных которого была обнаружена ошибка
   # @param error_name[String] - название ошибки
   # @param value[String] - значение поля, в котором обнаружена ошибка
   #
   def self.add_flk_error(name_object, error_name, value = nil)
      error_reference = ERROR_REFERENCE

      error = {}
      error[:error_id] = error_reference[error_name][:id]
      error[:description] = error_reference[error_name][:description]
      error[:name_object] = name_object
      error[:field] = error_reference[error_name][:field_name]
      error[:value] = value

      @error_list << error
      @errors += 1
   end


   # Отправляет запрос на сервис отчетов для генерации инвест. паспорта в формате pdf.
   # Затем копирует созданный файл на FTP-сервер
   #
   # @param invest_area_obj[Hash] - хэш с данными инвест. объекта
   # @param lang[String] - язык, на котором нужно сформировать инвест. паспорт
   # @param from_ftp[Boolean] - флаг, работать ли с файлами по ftp или локально
   #
   def self.generate_invest_passport_for_object(invest_area_obj, lang, from_ftp: nil)
      xls_path = XLS_PATH
      # target_path = [ROOT_FTP_FOLDER, TARGET_FTP_FOLDER].join('/')

      data = prepare_print_form_json(lang, invest_area_obj)

      puts JSON.generate(data)

      personal_id = invest_area_obj[:cadastre_number]

      StatisticReportsGroup.formate_html_passport(data[:content][:data][:investmentPassport],invest_area_obj[:details_hash],lang,personal_id)

      invest_area_obj[:details_hash]["pdf_file_name"] = personal_id.gsub('/','|').gsub(':','_') + ".html"
      invest_area_obj[:details_hash]["pdf_file_name_eng"] = personal_id.gsub('/','|').gsub(':','_') + "_eng.html"
      invest_area_obj.save

      remote_path_string = invest_area_obj[:details_hash]["source_summary_table_path"]

      target_path = "/mnt/ftp/" + [ROOT_FTP_FOLDER, TARGET_FTP_FOLDER].join('/')

      remote_path_arr = remote_path_string.split('/')
      remote_path_arr.pop

      remote_path = "/mnt/ftp#{remote_path_arr.join('/')}"

      photos = copy_photo_file_to_target(invest_area_obj, remote_path, target_path, from_ftp: from_ftp)

      insert_photos_in_object(invest_area_obj, photos)

      #TODO: заглушка для пдф паспортов
      return nil
      begin

         config = YAML.load_file(Rails.root.join('config','sokol_services.yml'))['reports']


         rsp = HTTParty.post(config['address'], :body => JSON.generate(data),
                             :headers => {
                                 'Content-Type' => 'application/json',
                             })
         if rsp.code.eql? 200
            if rsp.parsed_response != nil

               file_name = rsp.headers["content-disposition"].split("=").last

               file = File.open(
                   [
                       xls_path,
                       file_name
                   ].join('/'),
                   'w')

               file.write rsp.parsed_response.force_encoding('UTF-8')
               file.close unless file.nil?

               worked_file = [xls_path, file_name].join('/')

               remote_path_string = invest_area_obj[:details_hash]["source_summary_table_path"]
               if from_ftp
                  target_path = [ROOT_FTP_FOLDER, TARGET_FTP_FOLDER].join('/')

                  remote_path = remote_path_string.split('/')
                  remote_path.shift(1)
                  remote_path.pop

                  remote_path = convert_file_name(remote_path.join('/'), reverse:true)

                  check_ftp_connection
                  @ftp.putbinaryfile(worked_file, [target_path, file_name].join('/'))
               else
                  remote_path_arr = remote_path_string.split('/')
                  remote_path_arr.pop
                  target_path = "/mnt/ftp/" + [ROOT_FTP_FOLDER, TARGET_FTP_FOLDER].join('/')

                  remote_path = "/mnt/ftp#{remote_path_arr.join('/')}"

                  # move_file(worked_file, [target_path, file_name].join('/'))

                  truncated_file_name = truncate(file_name, 119)
                  move_file(worked_file, [target_path, truncated_file_name].join('/'))
                  FileUtils.rm(worked_file) if File.file?([target_path, truncated_file_name].join('/'))

               end

               if lang.eql?(:rus)
                  @rus += 1
                  photos = copy_photo_file_to_target(invest_area_obj, remote_path, target_path, from_ftp: from_ftp)
                  invest_area_obj[:details_hash]["pdf_file_name"] = file_name #if lang.eql?(:rus)
                  insert_photos_in_object(invest_area_obj, photos)
              end

               if lang.eql?(:eng)
                  @eng += 1
                  invest_area_obj[:details_hash]["pdf_file_name_eng"] = file_name
               end

               invest_area_obj.save
            end
         end
      rescue => err

         @logger.error(err)
      end
   end


   # Переводит некоторые позиции инвестиционного паспорта на англ. яз
   #
   # @param invest_obj[Hash] - хэш с данными инвест. объекта
   #
   def self.translate_to_eng(invest_obj)
      # engagement_term_list = ENGAGEMENT_TERM_LIST
      # engagement_term_list.each do |engagement_term|
      #    if invest_obj[:details_hash]["attraction_terms"].mb_chars.downcase.to_s.eql?(engagement_term[:name].mb_chars.downcase.to_s)
      #       invest_obj[:details_hash]["attraction_terms"] = engagement_term[:name_eng]
      #    end
      # end


      current_object_state_list = CURRENT_OBJECT_STATE_LIST
      current_object_state_list.each do |current_object_state|
         if invest_obj[:details_hash]["current_state"].mb_chars.downcase.to_s.eql?(current_object_state[:name].mb_chars.downcase.to_s)
            invest_obj[:details_hash]["current_state"] = current_object_state[:name_eng]
         end
      end

      ownership_type_list = OWNERSHIP_TYPE_LIST
      ownership_type_list.each do |ownership_type|
         if invest_obj[:details_hash]["ownership_type"].mb_chars.downcase.to_s.eql?(ownership_type[:name].mb_chars.downcase.to_s)
            invest_obj[:details_hash]["ownership_type"] = ownership_type[:name_eng]
         end
      end

      checked_fields_arr = CHECKED_FIELDS_LIST
      checked_fields_arr.each do |field_name|
         if invest_obj[:details_hash][field_name.to_s].to_s.mb_chars.downcase.eql?(NO_DATA_RUS.to_s.mb_chars.downcase)
            invest_obj[:details_hash][field_name] = NO_DATA_ENG
         end
      end

      using_site_suggestion_list = USING_SITE_SUGGESTION_LIST
      using_site_suggestion_list.each do |using_site_suggestion|
         if invest_obj[:details_hash]["use_types"].mb_chars.downcase.to_s.eql?(using_site_suggestion[:name].mb_chars.downcase.to_s)
            invest_obj[:details_hash]["use_types"] = using_site_suggestion[:name_eng]
         end
      end
   end


   # Добавляет секцию с информацией о фотографиях к хэшу инвест. объекта
   #
   # @param invest_area_obj[Hash] - хэш инвест. объекта
   # @param photo_array[Array] - массив с данными по фотографиям
   #
   def self.insert_photos_in_object(invest_area_obj, photo_array)
      photos = invest_area_obj[:details_hash]["photos"]

      if photos.present?
         photos.each do |photo_checked|
            photo_array.each do |photo_elem|

               x = photo_elem[:name].split('.')
               if x.is_a?(Array)
                  x.pop
                  photo_name = x.join('.')
               else
                  photo_name = photo_elem[:name]
               end


               y = photo_checked["remote_file_name"].split('/').last.split('.')
               if y.is_a?(Array)
                  y.pop
                  photo_checked_name = y.join('.')
               else
                  photo_checked_name = photo_checked["name"]
               end

               if photo_checked_name.present? && photo_checked_name.mb_chars.downcase.to_s.eql?(photo_name.mb_chars.downcase.to_s)
                  # photo_checked["path"] = photo_checked["name"] #[convert_file_name(remote_file_path), photo_elem[:file_name]].join('/')
                  photo_checked["name"] = photo_checked_name
                  photo_checked["phoenix_path"] = photo_elem[:file_name]
                  # invest_area_obj[:details_hash]["photos"]["phoenix_path"] = photo_elem[:file_name]
               end
            end
         end
      end
   end


   # Копирует фотографии инвест. объектов из районов в папку phoenix
   #
   # @param invest_area_obj[Hash] - хэш с данными инвест. объекта
   # @param remote_path[String] - путь, откуда копируются фотографии
   # @param target_path[String] - путь, куда копируются фотографии
   # @param from_ftp[Boolean] - флаг, копировать ли файлы с фтп или локально
   #
   # @return [Hash] хэш с данными по фотографиям
   #
   def self.copy_photo_file_to_target(invest_area_obj, remote_path, target_path, from_ftp: nil)
      xls_path = XLS_PATH
      photos_result = []

      photos = invest_area_obj[:details_hash]["photos"]

      if photos.present?
         check_ftp_connection
         photos.each do |photo|
            if photo["name"].present?
               res = photo["name"].split('/')
               if res.is_a?(Array)
                  photo["name"] = photo["name"].split('/').last
               end

               if from_ftp
                  file_on_ftp = file_exists_in_ftp(remote_path.split('/'), photo["name"].split('/').last)

                  if file_on_ftp[:result]
                     local_file_path = [xls_path , convert_file_name(file_on_ftp[:file_name])].join('/')


                     # begin
                     @ftp.getbinaryfile(["", remote_path.force_encoding("ASCII-8BIT"), file_on_ftp[:remote_file_path]].join('/'), local_file_path)
                     # rescue
                     #    byebug
                     #    @ftp.getbinaryfile([remote_path.force_encoding("UTF-8"), file_on_ftp[:file_name]].join('/'), local_file_path)
                     #
                     # end

                     ext = file_on_ftp[:file_name].split('.').last.downcase.to_s
                     dt = DateTime.now
                     file_name = [dt.strftime("%Y%m%d%H%M%S%L"), ext].join('.')
                     remote_new_file_path = [target_path, file_name].join('/')


                     @ftp.putbinaryfile(local_file_path, remote_new_file_path)

                     photos_result << {
                         file_name: file_name,
                         name: file_on_ftp[:file_name] #convert_file_name(file_on_ftp[:file_name])
                     }
                  end
               else
                  result_hash = file_exists_in_local(remote_path, photo["name"])

                  photo_name = result_hash[:file_name].split('/').last

                  if result_hash[:result]
                     local_file_path = [xls_path , photo_name].join('/')
                     FileUtils.cp(remote_path + '/' + photo_name, local_file_path)

                     ext = photo_name.split('.').last.downcase.to_s
                     dt = DateTime.now
                     file_name = [dt.strftime("%Y%m%d%H%M%S%L"), ext].join('.')

                     truncated_file_name = truncate(file_name, 119)
                     move_file(local_file_path, [target_path, truncated_file_name].join('/'))
                     FileUtils.rm(local_file_path) if File.file?([target_path, truncated_file_name].join('/'))

                     photos_result << {
                        file_name: file_name,
                        name: photo_name
                     }
                  end
               end
            end
         end
      end

      photos_result
   end



   # Формирует и возвращает hash с данными для печатной формы "Инвестиционный паспорт"
   #
   # @param lang[String] - язык, на котором нужно сформировать инвест. паспорт
   # @param invest_area_obj[Hash] - хэш с данными инвест. объекта
   #
   # @return [Hash] hash с данными для печатной формы "Инвестиционный паспорт"
   #
   def self.prepare_print_form_json(lang, invest_area_obj)
      invest_object = Marshal.load(Marshal.dump(invest_area_obj))

      basic_info_caption_arr = [:area_name_rus, :area_name_eng,:cadastre_number, :safe_area,
                                :current_state, :address, :total_land_footage, :total_oks_footage]

      legal_status_caption_arr = [:ownership_type, :attraction_terms, :preliminary_cost, :permitted_use]

      distance_caption_arr = [:auto_distance, :rails_distance]

      infrastructure_caption_arr = [:gas_description, :gas_available, :heating_description,
                                    :heating_available, :electric_description, :electric_available,
                                    :water_description, :water_available, :sewerage_description,
                                    :sewerage_available, :treatment_facilities_description, :treatment_facilities_available]

      additional_caption_arr = [:use_types, :description_rus, :description_eng]

      if lang.eql?(:eng)
         translate_to_eng(invest_object)
      end

      invest_area_detail_hash =
         invest_object[:details_hash].merge(
                                     area_name: invest_object[InvestArea.get_area_name_with_lang(lang)] , cadastre_number: invest_object[:cadastre_number]
         )

      {
          format: 'pdf',
          content: {
              meta: {
                  name: 'investment_passport',
                  caption: nil
              },
              data: {
                  investmentPassport: {
                      documentName: InvestArea.get_caption(lang, :document_name),
                      areaType: {
                          caption: InvestArea.get_caption(lang, :invest_area_type),
                          value: get_invest_area_type_name(invest_object, lang)  #InvestArea.get_value_from_details_hash(invest_area_detail_hash, :invest_area_type)
                      },
                      objectName: {
                          caption: InvestArea.get_caption(lang, :object_name),
                          value: InvestArea.get_value_from_details_hash(invest_area_detail_hash, InvestArea.get_area_name_with_lang(lang))
                      },
                      address: {
                          caption: InvestArea.get_caption(lang, :address),
                          value: InvestArea.get_value_from_details_hash(invest_area_detail_hash, :address)
                      },
                      propertyGroupList: [{
                                              id: '1',
                                              caption: InvestArea.get_caption(lang, :basic_info_group),
                                              propertyList: InvestArea.prepare_property_list(invest_area_detail_hash, basic_info_caption_arr, '1', lang)
                                          },
                                          {
                                              id: '2',
                                              caption: InvestArea.get_caption(lang, :legal_status_info),
                                              propertyList: InvestArea.prepare_property_list(invest_area_detail_hash, legal_status_caption_arr, '2', lang)
                                          },
                                          {
                                              id: '3',
                                              caption: InvestArea.get_caption(lang, :distance),
                                              propertyList: InvestArea.prepare_property_list(invest_area_detail_hash, distance_caption_arr, '3', lang)
                                          },
                                          {
                                              id: '4',
                                              caption: InvestArea.get_caption(lang, :infrastructure_info),
                                              propertyList: InvestArea.prepare_property_list(invest_area_detail_hash, infrastructure_caption_arr, '4', lang)
                                          },
                                          {
                                              id: '5',
                                              caption: InvestArea.get_caption(lang, :additional_info),
                                              propertyList: InvestArea.prepare_property_list(invest_area_detail_hash, additional_caption_arr, '5', lang)
                                          }]
                  }
              }
          }
      }

   end


   # Формирует массив с информацией по фотографиями для карты инвестиционных объектов
   #
   # @param photos_arr_source[Array] - массив с информацией по загруженным фотографиям объектов
   #
   # @return [Array] массив с информацией по фотографиями для карты инвестиционных объектов
   #
   def self.prepare_photos_hash(photos_arr_source)
      photo_array = []

      photos_arr_source.each do |photo|
         photo_array << {
             id: photo["id"],
             path: photo["phoenix_path"],
             description_rus: photo["description_rus"],
             description_eng: photo["description_eng"]
         }
      end

      return photo_array
   end


   # Возвращает первый не пустой номер телефона из списка контактов, или nil - в противном случае
   #
   # @param phones[Array] массив с номерами телефонов
   #
   # @return [String] номер телефона
   #
   def self.get_first_not_empty_phone_number(phones)
      phones.each do |phone|
         if phone["phone_num"].present?
            return phone["phone_num"]
         end
      end

      return nil
   end


   # Сохраняет все корректные инвест. объекты в БД
   #
   # @param invest_object_list[Array] - список корректных инвест. объектов
   #
   # @return [Boolean]
   #
   def self.save_objects(invest_object_list)
      # begin

         invest_object_list.each do |obj|
            if obj.present?
               format_phone_data(obj[:contacts_hash][:phones])

               obj[:details_hash][:invest_area_type_name] = obj[:details_hash][:invest_area_type]
               format_invest_area_type(obj) if obj[:area_type] == 1

               #save_gas_object(obj) if obj[:area_type].eql?(4)
               invest_area = InvestArea.new(obj)
               invest_area.save

               InvestStatistic.update_invest_statistic(obj)
            end
         end
         return true
      # rescue
      #    return false
      # end
   end

      #Метод записи газовых объектов в БД
      def self.save_gas_object(candidate_to_record)


         url = "http://10.22.60.247:8000/save_new_gas_object.json"
         query_params = {object_params: candidate_to_record}

         timeout = 30000
         Address.fias_request(url, query_params, timeout)
      end

   # Проверяет введенные координаты X и Y на корректность. Если координаты не верны,
   # то формирует ошибку для файла ошибок.
   #
   # @param obj[Hash] - хэш с данными инвест. объекта
   # @param type_area[String] - тип площадки
   #
   def self.check_coordinates(obj, type_area)
      result_x = InvestArea.check_coordinate_correct(obj[:details_hash][:cord_x])

      unless result_x
         if type_area.eql?(OBJECTS_AREA_TYPE)
            add_flk_error(obj[:area_name], :cord_x_invest_not_correct, obj[:cord_x])
         elsif type_area.eql?(POWER_AREA_TYPE)
            add_flk_error(obj[:area_name], :cord_x_power_not_correct, obj[:cord_x])
         else
            add_flk_error(obj[:area_name], :cord_x_field_not_correct, obj[:cord_x])
         end
      end

      result_y = InvestArea.check_coordinate_correct(obj[:details_hash][:cord_y])

      unless result_y
         if type_area.eql?(OBJECTS_AREA_TYPE)
            add_flk_error(obj[:area_name], :cord_y_invest_not_correct, obj[:cord_y])
         elsif type_area.eql?(POWER_AREA_TYPE)
            add_flk_error(obj[:area_name], :cord_y_power_not_correct, obj[:cord_y])
         else
            add_flk_error(obj[:area_name], :cord_y_field_not_correct, obj[:cord_y])
         end
      end

      return result_x && result_y
   end


   # Убирает ".0" из номера телефона. ".0" появляется при чтении данных из ячейки excel;
   #
   # @param phones[Array] - массив с номерами телефонов
   #
   def self.format_phone_data(phones)
      phones.each do |phone|
         phone[:phone_num] = phone[:phone_num].sub('.0', '')
      end
   end


   # Получает значение id типа площадки инвест. объекта по его имени из справочника
   #
   # @param obj[Hash] - хэш с данными инвест. объекта
   #
   def self.format_invest_area_type(obj)
      invest_area_type = obj[:details_hash][:invest_area_type]

      type_area_list = TYPE_AREA_LIST

      type_area_list.each do |type_area|
         if invest_area_type.eql?(type_area[:name])
            obj[:details_hash][:invest_area_type] = type_area[:id]
         end
      end
   end



   # Метод возвращает прогрессбар с заданными настройками
   #
   # @param options[Hash] - настройки прогрессбара.
   #
   # @return ProgressBar
   #
   def self.get_progressbar(options={})
      title = options[:title]
      total = options[:total]
      length = options[:length]
      format = options[:format]

      ProgressBar.create(
          :title => "#{ap title}",
          :progress_mark => '•',
          :remainder_mark => '·',
          :total => total || nil,
          :length => length || 180,
          :format => format || '|%B| %a %t Обработано %c файлов')
   end



   # Создает структуру каталогов, если они отсутствуют
   #
   # @param path[String] - путь к конечному каталогу
   #
   def self.check_path(path)
      FileUtils.mkdir_p path
   end



   # Метод-предикат возвращает true, если в имени файла имеется исключающий
   #  суффикс/постфикс заданный в EXCLUDE_SUFFIX.
   # Используется для определения ислючить ли файл из обработки
   #
   # @param file_name[String] - имя файла.
   #
   # @return [Boolean]
   #
   def self.is_exclude(file_name)
      excludes = EXCLUDE_SUFFIX
      excludes.each do |exclude|
         return true if (file_name.include? exclude)
      end

      false
   end



   # Сбрасывает (очищает) все данные по загруженным мощностям в БД. Очищаются таблицы
   # invest_areas, invest_area_properties, invest_statistics в схеме invest
   #
   def self.reset_import
      time_start = Time.now

      # Очищаем таблицу принятых объектов и сбрасываем sequence
      InvestArea.all.each {|record| record.delete}
      ActiveRecord::Base.connection.reset_pk_sequence!('invest_areas')

      InvestAreaProperty.all.each {|record| record.delete}
      ActiveRecord::Base.connection.reset_pk_sequence!('invest_area_properties')

      InvestStatistic.all.each {|record| record.delete}
      ActiveRecord::Base.connection.reset_pk_sequence!('invest_statistics')

      reset_files_on_ftp

      past_time = Time.now - time_start
      @logger.info(["Удаление заняло:", past_time, "секунд"].join(" "))
   end


   # Сбрасывает (обнуляет) в БД статистику по мощностям
   #
   def self.reset_power_statistic
      invest_statistic = InvestStatistic.get_statistic_kind

      InvestAreaProperty.all.each do |record|
         if record.invest_area[:area_type] == 2
            record.delete
         end
      end

      InvestArea.all.each do |record|
         if record[:area_type] == 2
            record.delete
         end
      end

      stat_count_all = get_stat_count_all_without_power(invest_statistic)

      if stat_count_all.present?
         statistic_obj = InvestStatistic.where('node_ao_guid = ? and statistic_type = ?', REGION_GUID, invest_statistic[:stat_count_all][:code]).first
         statistic_obj[:count] = stat_count_all
         statistic_obj.save
      end

      statistic_id_arr = get_power_statistic_id_arr(invest_statistic)

      power_statistic_list = InvestStatistic.where(statistic_type: statistic_id_arr)

      power_statistic_list.each do |record|
         record[:count] = 0
         record.save
      end
   end



   # Формирует массив с id объектов-мощностей
   #
   # @param invest_statistic[Hash] - хэш с видами статистик
   #
   # @return [Array] массив с id объектов-мощностей
   #
   def self.get_power_statistic_id_arr(invest_statistic)
      statistic_id_arr = []

      invest_statistic.each do |statistic|
         if statistic[1][:is_power]
            statistic_id_arr << statistic[1][:code]
         end
      end

      return statistic_id_arr
   end


   # Подсчитывает кол-во загруженных в БД инвест объектов, без учета мощностей
   #
   # @param invest_statistic[Hash] - хэш с видами статистик
   #
   # @return [Integer] - кол-во загруженных в БД инвест объектов, без учета мощностей
   #
   def self.get_stat_count_all_without_power(invest_statistic)
      power_total = InvestStatistic.where('node_ao_guid = ? and statistic_type = ?', REGION_GUID, invest_statistic[:stat_count_power][:code]).first
      all_total = InvestStatistic.where('node_ao_guid = ? and statistic_type = ?', REGION_GUID, invest_statistic[:stat_count_all][:code]).first

      if power_total.present? && all_total.present?
         return all_total[:count] - power_total[:count]
      end

      return nil
   end


   # Удляет постфиксы _COMPLETE и _ERROR из имен файлов на ftp-сервере
   #
   def self.reset_files_on_ftp
      @ftp = ftp_connect
      @ftp.chdir(ROOT_FTP_FOLDER)
      @ftp.chdir(TARGET_FTP_FOLDER)

      # rename_files_on_ftp(root_folder: ROOT_FTP_FOLDER)
      explore_ftp_for_remove_files(exts: ['.pdf', '.txt', 'jpg', '.jpeg'])

      @ftp.close
   end


   # Возвращет наименование типа площадки из справочника в зависимости от заданного языка
   #
   # @param obj[Hash] - хэш с данными инвест. объекта
   # @param lang[String] - язык
   #
   def self.get_invest_area_type_name(obj, lang)
      type_area_list = TYPE_AREA_LIST

      type_area_list.each do |type_area|
         if obj[:details_hash].present? && obj[:details_hash]['invest_area_type'].present?
            if obj[:details_hash]['invest_area_type'] == type_area[:id]
               if lang.eql?(:rus)
                  return type_area[:name]
               else
                  return type_area[:name_eng]
               end
            end
         end
      end
   end


   # Копирует файл ошибок в локальную папку
   #
   # @param local_path[String] - локальный путь, куда должен быть положен файл
   # @param testing[Boolean] - флаг тестового режима
   # @param folder[String] - папка, где располжен исходный файл
   # @param file_name[String] - Имя исходного файла
   # @param is_power[Boolean] - флаг, относится ли инвест. объект к мощностям
   #
   def self.puts_local_error_file(local_path, testing: false, folder:nil, file_name: nil, is_power: false)

      local_file_name = 'errors.xlsx'
      unless file_name.present?
         remote_file_name = local_file_name
      end

      if testing
         errors_dir = "#{ROOT_PATH}/imports/inventory-test"
      else
         path = folder
         path = folder.split('/') if folder.is_a? String

         begin
            if is_power
               err_path = path.shift(5)
            else
               err_path = path.shift(6)
            end
         rescue
            byebug
         end
         errors_dir = [err_path, 'ERR'].join('/')
      end

      check_path(errors_dir)

      dt = DateTime.now
      str_dt = dt.strftime("%Y%m%d%H%M")
      f_name = file_name.sub('.xlsx', '').sub('.xls', '')

      new_file_name = [[path.join('_'), f_name, 'ERRORS', str_dt].join('_'), 'xlsx'].join('.')

      write_errors_to_file(@error_list, local_file_name, file_name)

      begin
         # move_file([local_path, local_file_name].join('/'), [errors_dir, new_file_name].join('/'))

         truncated_file_name = truncate(new_file_name, 119)
         move_file([local_path, local_file_name].join('/'), [errors_dir, truncated_file_name].join('/'))
         FileUtils.rm([local_path, local_file_name].join('/')) if File.file?(truncated_file_name)
      rescue
         byebug
      end
   end


   # Копирует файл с данными статистики по инвест-объектам в папку shared
   #
   # @param [Array] statistic - массив статистик
   #
   def self.puts_full_statistic_to_file(statistic)
      xls_path = XLS_PATH
      file_name = 'Статистика_инвест_объектов_по_файлам.xlsx'

      dt = DateTime.now
      str_dt = dt.strftime("%Y%m%d_%H%M")

      file_name = file_name.sub('.xlsx', "_#{str_dt}.xlsx")

      file_path = [xls_path, file_name].join('/')

      write_statistic_to_file(statistic, file_name)

      path = "/mnt/ftp/"
      put_file_to_shared(file_path, path, file_name)
   end


   # Создает новый xls-файл и записывает в него статистику
   #
   # @param statistic[Array] - массив статистик
   # @param file_name[String] - имя файла
   #
   # @return nil
   #
   def self.write_statistic_to_file(statistic, file_name)

      file_path = [XLS_PATH, file_name].join('/')
      workbook = WriteXLSX.new(file_path)

      stat_sheet = workbook.add_worksheet(ERROR_SHEET_NAME)

      stat_sheet.write_row('A1', ['','Всего',
                                  '', '', '',
                                  'Файлы:'])
      stat_sheet.write_row('A2', ['Район','Объектов',
                                  'Принято', 'Отклонено', 'Не обработано',
                                  'Всего', 'Принято', 'Отклонено', 'Описаний ошибок', 'Не обработано', 'Проигнорировано', 'Не удалось прочесть'])

      row = 3

      statistic.each do |stat|

         res = [stat[:district], stat[:stat][:objects][:count],
                stat[:stat][:objects][:success], stat[:stat][:objects][:error], stat[:stat][:objects][:not_processed],
                stat[:stat][:files][:count],
                stat[:stat][:files][:success], stat[:stat][:files][:error],
                stat[:stat][:files][:error_info], stat[:stat][:files][:not_processed],
                stat[:stat][:files][:excluded], stat[:stat][:files][:read_error]
         ]

         stat_sheet.write_row('A' + row.to_s, res)
         row += 1
      end

      workbook.close
   end


   # Метод проверяет существование файла local_file_name в каталоге xls_path,
   # если файл найден наращивает index, приклеивает его к имени файла и ищет снова,
   # иначе возвращает имя файла
   #
   # @param local_file_name[String] - имя файла.
   # @param index[Int] - индекс (на входе не задан).
   #
   # @return [String] - имя файла, не существующего в каталоге
   #
   def self.check_local_file_name(local_file_name, index = 0)
      xls_path = XLS_PATH
      if index == 0
         local_file_path = [xls_path , local_file_name].join('/')
      else
         local_file_path = [xls_path , local_file_name.sub(".xls", "#{index}.xls")].join('/')
      end

      if File.file?(local_file_path)
         index += 1
         local_file_name = check_local_file_name(local_file_name, index)

      else
         local_file_name = local_file_name.sub(".xls", "#{index}.xls")
      end

      local_file_name
   end


   # Возвращает обрезанную строку по заданной длине, по-умолчанию 225 + 30 символов
   # @param string[String] - заданная строка
   # @param length[Integer] - длина строки
   #
   # @return [String] обрезанная строка
   #
   def self.truncate(string, length = 225)
      string.size > length + 30 ? [string[0,length],string[-30,30]].join("") : string
   end

   # Метод устанавливает начальные значения для массива статистик @pass_stat
   #
   # @return nil
   #
   def self.clear_pass_stat
      @pass_stat = {
          :objects => {
              :count => 0,
              :success => 0,
              :error => 0,
              :not_processed => 0
          },
          :files => {
              :count => 0,
              :success => 0,
              :error => 0,
              :error_info => 0,
              :not_processed => 0,
              :excluded => 0,
              :read_error => 0
          }
      }
   end

   # Получаем форматированную строку затраченного времени
   #
   # @param seconds[Integer] - количество секунд
   #
   # @return [String]
   def self.seconds_to_units(seconds)
      '%d days, %d hours, %d min, %d sec' %
          [24,60,60].reverse.inject([seconds]) {|result, unitsize|
             result[0,0] = result.shift.divmod(unitsize)
             result
          }
   end

      # 0 - offer
      # 1 - property_type
      # 2 - condition
      # 3 - area_type
      def self.translate_filtered_fields(category,name_in_category)
        names_list =  USING_SITE_SUGGESTION_LIST if category.eql?(0)
        names_list =  OWNERSHIP_TYPE_LIST if category.eql?(1)
        names_list =  CURRENT_OBJECT_STATE_LIST if category.eql?(2)
        names_list =  TYPE_AREA_LIST if category.eql?(3)
         names_list.each do |name|
            return name[:name_eng] if name_in_category.eql?(name[:name])
         end
      end



   #Метод формирующий резервную копию данных на 8.8
   # Формирует копию папки current с указанием даты формирования резервной копии
   def self.backup_data
      smb = "/mnt/sm008"
      production_file = "invest.txt"
      folder_with_data = "images"
      actual_ver = "CURRENT_DATA"

      check_local_directories("10.22.8.8/sokol","sm008")
      main_folder = [smb,folder_with_data,actual_ver].join("/")
      static_data = [smb,production_file].join("/")

      actual_folder = main_folder + "_#{Time.now.to_s[0..9]}"

      move_file(main_folder,actual_folder)

      actual_file = [smb,"invest_#{Time.now.to_s[0..9]}.txt"].join("/")

      move_file(static_data,actual_file)



      backups_group = {}

      #Составление рейтинга резервных копий
      Dir.entries([smb,folder_with_data].join("/")).select{|qwe| qwe.include?("CURRENT_DATA")}.each do |backup|
         separated = backup[13..22].to_s.split("-")
         backups_group[backup] = define_date_rating(separated)
      end

      actual_file_names = update_backup(backups_group)
      actualize_backups(actual_file_names,[smb,folder_with_data].join("/"))
      copy_data
   end

      #Метод подсчета рейтинга версии резервной копии
      def self.define_date_rating(date)
         total_points = date[0].to_i*10 + date[1].to_i*32 + date[2].to_i
         return total_points
      end

      #Метод выборки 3х актуальных резервных копий.
      #Остальные копии удаляются с хранилища.
      def self.update_backup(date_components)
         return date_components.sort_by{|name,points|  points}.reverse.to_h.keys[0..2]
      end


      #Метод удаляет старые резервные копии json'а
      def self.actualize_backups(actual_list,folder)

         Dir.chdir(folder)
         Dir.glob('*').select {|f| File.directory? f}.each do |file|

            if actual_list.exclude?(file)
               FileUtils.rm_rf([folder,file].join("/"))
               puts("deleted #{file}")
            end
         end
      end


      #Копирует данные с ftp на 8.8
      def self.copy_data
         folder_from_ftp = "phoenix"
         folder_from_prod = "CURRENT_DATA"
         ftp_folder = ["/mnt/ftp",ROOT_FTP_FOLDER].join("/")
         prodution_folder = "/mnt/sm008"
         backups_folder  = "images"
         phoenix_json = "common.txt"
         check_local_directories("10.22.1.231/FTP","ftp")

         FileUtils.cp_r([ftp_folder,folder_from_ftp].join("/"),"#{[prodution_folder,backups_folder].join("/")}/") # Перемещение папки на 8.8

         move_file([prodution_folder,backups_folder,folder_from_ftp].join("/"),[prodution_folder,backups_folder,folder_from_prod].join("/"))

         FileUtils.cp([prodution_folder,folder_from_prod,phoenix_json].join("/"),prodution_folder) # размещение json на прод

         move_file("/mnt/sm008/common.txt","/mnt/sm008/invest.txt")

      end

      #Метод возвращает поля
      def self.prepare_suggestions_for_using_list(obj)
         list = USING_SITE_SUGGESTION_LIST
            list.each do |suggestion|
               next if obj[:data].keys.include?(:"#{suggestion[:name_eng]}")
               obj[:data][:"#{suggestion[:name_eng]}"]  = prepare_json_part("#{suggestion[:name]}",
                                                                        "#{suggestion[:name_eng].gsub('_',' ').capitalize}","-",false,"","text")
            end
         return obj
      end

      def self.prepare_property_types_list(obj)
         types_list = OWNERSHIP_TYPE_LIST
         types_list.each do |property_type|
            next if obj[:data].keys.include?(:"#{property_type[:name_eng]}")
            obj[:data][:"#{property_type[:name_eng]}"]  = prepare_json_part("#{property_type[:name]}",
                                                                         "#{property_type[:name_eng].gsub('_',' ').capitalize}","-",false,"","text")
         end
         return obj
      end

      def self.check_local_directories(dir_to_mount,local_dir)

         login_password =
            {
               '10.22.23/FTP': {login: 'sokol', password: 'Dtr'},
               '10.22.1/shared':{login: 'sokol_dev',password: 'Dt'}
            }

         log_in = login_password[:"#{dir_to_mount}"]

         if system ("mount | grep #{local_dir}")
            return true
         else
         system "echo deployer | sudo -S mount -t cifs -o username=#{log_in[:login]},password=#{log_in[:password]},iocharset=utf8,file_mode=0777,dir_mode=0777 //#{dir_to_mount} /mnt/#{local_dir}"
         system ("mount | grep #{local_dir}")
         end
      end

      #Группа методов получения информации о газораспределительном слое, закомментирован по причине несоответствия данных
      # требованиям ФЛК
      # def self.save_gas_supplies_data(obj)
      #    parsed_remote_data = recieve_gas_data.to_a
      #    names_space = parsed_remote_data[3]
      #    parsed_remote_data[5..parsed_remote_data.count - 1].each do |gas_point|
      #       obj << format_parsed_data(gas_point)
      #    end
      # end
      #
      # def self.recieve_gas_data
      #    gas_supplies = "http://ufa-tr.gazprom.ru/d/textpage/63/99/informatsiya-o-nalichii-svobodnoj-propusknoj-sposobnosti-grs-(po.xlsx"
      #    return Roo::Spreadsheet.open(gas_supplies)
      # end
      #
      # def self.format_parsed_data(gas_supply)
      #    return
      #    {
      #       supply_name: gas_supply[1],
      #       project_power: gas_supply[2],
      #       actual_performance: gas_supply[3],
      #       value_to_connection: gas_supply[4],
      #       bandwidth: gas_supply[5],
      #       bandwidth_increasing_deadlines: gas_supply[6],
      #       increasing_paramethers: gas_supply[7]
      #
      #    }
      # end



   TYPE_AREA_LIST =
      [
         { id: 1, name: 'Модуль с прилегающими бытовыми помещениями', name_eng: 'Module_with_adjoining_premises' },
         { id: 2, name: 'Земельные участки', name_eng: 'Land' },
         { id: 3, name: 'Территория незавершенного строительства', name_eng: 'Territory of unfinished construction' },
         { id: 4, name: 'Складское помещение', name_eng: 'Warehouse space' },
         { id: 5, name: 'Производственная база (перечень оборудования)', name_eng: 'Industrial base' },
         { id: 6, name: 'Здание предприятия (наименование)', name_eng:  'Enterprise buildings' },
         { id: 7, name: 'Помещение', name_eng: 'Enterprise' },
         { id: 8, name: 'Бесхоз', name_eng: 'Derelict property' },
         { id: 9, name: 'Иное', name_eng: 'Other' }
      ]


   CURRENT_OBJECT_STATE_LIST =
      [
         { id: 1, name: 'Требует вложения средств', name_eng: 'requires capital investment' },
         { id: 2, name: 'Необходим ремонт', name_eng: 'repair is necessary' },
         { id: 3, name: 'Продан по торгам', name_eng: 'Sold by auction' },
         { id: 4, name: 'Реализован субъектом МСП', name_eng: 'Implemented by MSP' },
         { id: 5, name: 'Передан в аренду', name_eng: 'Leased out' },
         { id: 6, name: 'Передан в аренду МСП', name_eng: 'Leased to MSP' }
      ]


   OWNERSHIP_TYPE_LIST =
      [
         { id: 1, name: 'Муниципальная', name_eng: 'municipal' },
         { id: 2, name: 'Частная', name_eng: 'private' },
         { id: 3, name: 'Государственная', name_eng: 'state' },
      ]


   # ENGAGEMENT_TERM_LIST =
   #    [
   #       { id: 1, name: 'краткосрочная аренда', name_eng: 'short-term lease' },
   #       { id: 2, name: 'долгосрочная аренда', name_eng: 'long-term lease' },
   #       { id: 3, name: 'выкуп', name_eng: 'ransom' },
   #       { id: 4, name: 'совместная реализация инвестиционных проектов', name_eng: 'joint implementation of investment projects' }
   #    ]


   USING_SITE_SUGGESTION_LIST =
      [
         { id: 1, name: 'Сельское хозяйство', name_eng: 'agriculture' },
         { id: 2, name: 'Животноводство', name_eng: 'stock_raising' },
         { id: 3, name: 'Промышленное производство', name_eng: 'industry' },
         { id: 4, name: 'Транспорт и хранение', name_eng: 'transport_and_storage' },
         { id: 5, name: 'Индустриальные парки', name_eng: 'industrial_parks' },
         { id: 6, name: 'Технопарки', name_eng: 'technoparks' },
         { id: 7, name: 'Отдых и туризм', name_eng: 'recreation_and_tourism' },
         { id: 8, name: 'Оптовая и розничная торговля', name_eng: 'wholesale_and_retail_trading' },
         { id: 9, name: 'Питание', name_eng: 'nutrition' },
         { id: 10, name: 'Образование', name_eng: 'education' },
         { id: 11, name: 'Здравоохранение', name_eng: 'health_care' },
         { id: 12, name: 'Общественно-деловое значение', name_eng: 'social_business_purpose' },
         { id: 13, name: 'Инженерные коммуникации', name_eng: 'engineering_communications' },
         { id: 14, name: 'Индустриальный парк типа greenfield', name_eng: 'greenfield_type_industrial_park' },
         { id: 15, name: 'Индустриальный парк типа brownfield', name_eng: 'brownfield_type_industrial_park' },
         { id: 16, name: 'Иное', name_eng: 'other' },
      ]


   CHECKED_FIELDS_LIST = [:safe_area, :current_state, :address, :total_land_footage, :cadastre_number,
                          :total_oks_footage, :ownership_type, :attraction_terms, :preliminary_cost,
                          :permitted_use, :auto_distance, :rails_distance, :gas_description,
                          :gas_available, :heating_description, :heating_available, :electric_description,
                          :electric_available, :water_description, :water_available, :sewerage_description,
                          :sewerage_available, :treatment_facilities_description, :treatment_facilities_available,
                          :use_types, :description, :description_rus
   ]


   # Диапазоны кодов ошибок/home/bulychev_ay/all_correct.xlsx
   # 100 - 199 - Не заполнены поля
   # 200 - 299 - Прочие ошибки
   ERROR_REFERENCE = {
      area_name_empty: {
         id: 100,
         description: 'Не заполнено поле Наименование объекта, краткая характеристика на РУССКОМ',
         field_name: 'Наименование объекта, краткая характеристика на РУССКОМ',
         type_area: 1,
         is_power: false
      },
      email_invest_is_empty: {
         id: 101,
         description: 'Не заполнено поле "Электронная почта" на вкладке "Контакты"',
         field_name: 'Электронная почта',
         type_area: 1,
         is_power: false
      },
      phone_invest_is_empty: {
         id: 102,
         description: 'Должен быть заполнен хотя бы один номер телефона на вкладке "Контакты"',
         field_name: 'Телефонные номера для связи',
         type_area: 1,
         is_power: false
      },
      region_is_empty: {
         id: 103,
         description: 'Не заполнено поле "Номер района" на вкладке "Контакты"',
         field_name: 'Номер района',
         type_area: 1,
         is_power: false
      },
      village_council_is_empty: {
         id: 104,
         description: 'Не заполнено поле "Номер сельсовета" на вкладке "Контакты"',
         field_name: 'Номер сельсовета',
         type_area: 1,
         is_power: false
      },
      locality_is_empty: {
         id: 105,
         description: 'Не заполнено поле "Номер населенного пункта" на вкладке "Контакты"',
         field_name: 'Номер населенного пункта',
         type_area: 1,
         is_power: false
      },
      invest_area_type_is_empty: {
         id: 106,
         description: 'Не заполнено поле "Тип площадки"',
         field_name: 'Тип площадки',
         type_area: 1,
         is_power: false
      },
      company_name_is_empty: {
         id: 107,
         description: 'Не заполнено поле "Наименование организации"',
         field_name: 'Наименование организации',
         type_area: 2,
         is_power: true
      },
      area_name_power_empty: {
         id: 108,
         description: 'Не заполнено поле "Наименование объекта"',
         field_name: 'Наименование объекта',
         type_area: 2,
         is_power: true
      },
      photo_file_not_found: {
         id: 109,
         description: 'Фотография не найдена. Проверьте её наличие. Файлы фотографий должны находиться в папке с инвестиционной картой. Если в имени файла фото имеются буквы "я" - замените их на "Я"',
         field_name: 'Фото 1 / Фото 2',
         type_area: 1,
         is_power: false
      },
      email_power_is_empty: {
         id: 110,
         description: 'Не заполнено поле "Электронная почта" на вкладке "Контакты"',
         field_name: 'Электронная почта',
         type_area: 2,
         is_power: true
      },
      phone_power_is_empty: {
         id: 111,
         description: 'Должен быть заполнен хотя бы один номер телефона на вкладке "Контакты"',
         field_name: 'Телефонные номера для связи',
         type_area: 2,
         is_power: true
      },
      photos_not_complete: {
         id: 112,
         description: 'К инвестиционному объекты должны быть приложены 4 фотографии',
         field_name: 'Фото 1, Фото 2, Фото внутри объекта 1, Фото внутри объекта 2',
         type_area: 1,
         is_power: false
      },
      field_name_is_empty: {
         id: 113,
         description: 'Не заполнено поле "Месторождение"',
         field_name: 'Месторождение',
         type_area: 3,
         is_power: false
      },
      field_name_eng_is_empty: {
         id: 114,
         description: 'Не заполнено поле "Месторождение (на англ. яз)"',
         field_name: 'Месторождение (на англ. яз)',
         type_area: 3,
         is_power: false
      },
      use_is_empty: {
         id: 115,
         description: 'Не заполнено поле "Полезное ископаемое, применение"',
         field_name: 'Полезное ископаемое, применение',
         type_area: 3,
         is_power: false
      },
      license_is_empty: {
         id: 116,
         description: 'Не заполнено поле "Лицензия"',
         field_name: 'Лицензия',
         type_area: 3,
         is_power: false
      },
      license_eng_is_empty: {
         id: 117,
         description: 'Не заполнено поле "Лицензия (на англ. яз)"',
         field_name: 'Лицензия (на англ. яз)',
         type_area: 3,
         is_power: false
      },
      type_unit_is_empty: {
         id: 118,
         description: 'Не заполнено поле "Единица измерения"',
         field_name: 'Единица измерения',
         type_area: 3,
         is_power: false
      },
      field_passport_not_found: {
         id: 119,
         description: 'Файл паспорта месторождения не найден',
         field_name: 'Файл паспорта месторождения',
         type_area: 3,
         is_power: false
      },
      suggestion_for_using_dont_correct:{
         id:120,
         description: 'Указаный предложение по искальзованию не соответствует требуемым',
         field_name: 'Предложение по использованию',
         type_area: 1,
         is_power: false
      },
      condition_dont_correct:{
         id: 121,
         description: 'Указанное состояние объекта не соответствует требуемым',
         field_name: 'Текущее состояние объекта',
         type_area: 1,
         is_power: false
      },
      cord_x_invest_not_correct: {
         id: 200,
         description: 'Не верно указана координата объекта',
         field_name: 'Координата Х',
         type_area: 1,
         is_power: false
      },
      cord_y_invest_not_correct: {
         id: 201,
         description: 'Не верно указана координата объекта',
         field_name: 'Координата Y',
         type_area: 1,
         is_power: false
      },
      cord_invest_out_of_bounds: {
         id: 202,
         description: 'Указанные координаты (или одна из координат) находятся за пределами Башкортостана',
         field_name: 'Координата Х, Координата Y',
         type_area: 1,
         is_power: false
      },
      cord_invest_invalid_value: {
         id: 209,
         description: 'Введены некорректные данные в полях "Координата Х" и/или "Координата Y"',
         field_name: 'Координата Х, Координата Y',
         type_area: 1,
         is_power: false
      },
      object_file_version_err: {
         id: 203,
         description: ['Версия документа должна быть не ниже', OBJECTS_MINIMAL_FILE_VERSION].join(' '),
         field_name: 'Версия сводной таблицы',
         type_area: 1,
         is_power: false
      },
      power_file_version_err: {
         id: 204,
         description: ['Версия документа должна быть не ниже', POWER_MINIMAL_FILE_VERSION].join(' '),
         field_name: 'Версия сводной таблицы',
         type_area: 2,
         is_power: true
      },
      contacts_sheet_not_found: {
         id: 205,
         description: 'Не найдена вкладка "Контакты"',
         field_name: '',
         type_area: 1,
         is_power: false
      },
      cord_x_power_not_correct: {
         id: 206,
         description: 'Не верно указана координата объекта',
         field_name: 'Координата Х',
         type_area: 2,
         is_power: true
      },
      cord_y_power_not_correct: {
         id: 207,
         description: 'Не верно указана координата объекта',
         field_name: 'Координата Y',
         type_area: 2,
         is_power: true
      },
      cord_power_out_of_bounds: {
         id: 208,
         description: 'Указанные координаты (или одна из координат) находятся за пределами Башкортостана',
         field_name: 'Координата Х, Координата Y',
         type_area: 2,
         is_power: true
      },
      cord_power_invalid_value: {
         id: 210,
         description: 'Введены некорректные данные в полях "Координата Х" и/или "Координата Y"',
         field_name: 'Координата Х, Координата Y',
         type_area: 2,
         is_power: true
      },
      area_name_eng_invest_cyrillic_error: {
         id: 211,
         description: 'Поле содержать только латинские буквы м знаки пунктуации',
         field_name: 'Наименование объекта, кратная характеристика НА АНГЛИЙСКОМ',
         type_area: 1,
         is_power: false
      },
      description_eng_invest_cyrillic_error: {
         id: 212,
         description: 'Поле содержать только латинские буквы м знаки пунктуации',
         field_name: 'Дополнительная информация о площадке НА АНГЛИЙСКОМ',
         type_area: 1,
         is_power: false
      },
      invest_type_area_invalid_value: {
         id: 213,
         description: 'Значение поля не соответствует ни одному из значений из справочника (выпадающего списка)',
         field_name: 'Тип площадки',
         type_area: 1,
         is_power: false
      },
      cord_x_field_not_correct: {
         id: 214,
         description: 'Не верно указана координата объекта',
         field_name: 'Координата Х',
         type_area: 3,
         is_power: false
      },
      cord_y_field_not_correct: {
         id: 215,
         description: 'Не верно указана координата объекта',
         field_name: 'Координата Y',
         type_area: 3,
         is_power: false
      },
      cord_field_out_of_bounds: {
         id: 216,
         description: 'Указанные координаты (или одна из координат) находятся за пределами Башкортостана',
         field_name: 'Координата Х, Координата Y',
         type_area: 3,
         is_power: false
      },
      field_file_version_err: {
         id: 217,
         description: ['Версия документа должна быть не ниже', FIELD_MINIMAL_FILE_VERSION].join(' '),
         field_name: 'Версия сводной таблицы',
         type_area: 3,
         is_power: true
      },
      not_uniq_object_err: {
         id: 218,
         description: 'Не уникальный объект. В базу данных ранее уже был загружен объект с этим кадастровым номером',
         field_name: 'Кадастровый номер ЗУ или ОКС',
         type_area: 1,
         is_power: false
      },
      cadastre_number_err: {
         id: 219,
         description: 'Не верно указан кадастровый номер',
         field_name: 'Кадастровый номер',
         type_area: 1,
         is_power: false
      },
      not_uniq_object_in_list_err: {
         id: 220,
         description: 'Не уникальный объект. В файле обнаружено несколько объектов с одним кадастровым номером',
         field_name: 'Кадастровый номер ЗУ или ОКС',
         type_area: 1,
         is_power: false
      },
      error_copy_file: {
         id: 501,
         description: 'Ошибка! Невозможно прочитать файл. Откройте этот файл и сохраните его снова. Если в имени файла имеется буква "я" замените ее на "Я"',
         field_name: ''
      },
      error_read_file: {
         id: 502,
         description: 'Ошибка! При чтении файла возникла ошибка. Откройте этот файл и сохраните его снова.',
         field_name: ''
      },
      error_format_file: {
         id: 503,
         description: 'Ошибка! Формат файла отличается от его содержимого. Сохраните файл в требуемом формате, без изменения расширения.',
         field_name: ''
      },
      objects_count_err: {
         id: 601,
         description: 'Файл не содержит объектов',
         field_name: ''
      }
   }


