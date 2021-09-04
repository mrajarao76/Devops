# Swap memory
#swapoff -a

# Install Jinkins

sudo apt-get update

wget -q -O - https://pkg.jenkins.io/debian-stable/jenkins.io.key | sudo apt-key add -

sudo sh -c 'echo deb https://pkg.jenkins.io/debian-stable binary/ > \
    /etc/apt/sources.list.d/jenkins.list'

sudo apt-get update

sudo apt-get install jenkins -y





#Update the repositories
sudo apt update


#search of all available packages:
sudo apt search openjdk

#Pick one option and install it:
sudo apt install openjdk-8-jdk -y

#Confirm installation:
sudo apt install openjdk-8-jdk

#checking installation:
java -version


###################Start Jenkins########################
Register the Jenkins service with the command:

sudo systemctl daemon-reload

#You can start the Jenkins service with the command:
sudo systemctl start jenkins

#You can check the status of the Jenkins service using the command:
sudo systemctl status jenkins













