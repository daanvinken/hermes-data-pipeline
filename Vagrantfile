Vagrant.configure("2") do |config|

config.vm.box = "mattburnett/fedora-35-m1-arm64"
  config.vm.provider "parallels" do |v|
    v.memory = "10240"
    v.cpus = 6
  end
  config.vm.synced_folder ".", "/vagrant", owner: "vagrant",
                                           group: "vagrant"

  config.ssh.insert_key = true

 config.vm.network "forwarded_port", guest: 8081, host: 8081 # Spark UI
  config.vm.network "forwarded_port", guest: 8080, host: 8080 # Spark misc
  config.vm.network "forwarded_port", guest: 7077, host: 7077 # Spark misc

  config.vm.provision "shell", path: "provision.sh"
end

