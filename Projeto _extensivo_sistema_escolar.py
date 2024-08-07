from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Iniciar a sessão Spark
spark = SparkSession.builder \
    .appName("Sistema de Gerenciamento de Alunos") \
    .getOrCreate()

# Definindo esquema para os dados dos alunos
schema = StructType([
    StructField("Nome", StringType(), True),
    StructField("Nota1", FloatType(), True),
    StructField("Nota2", FloatType(), True),
    StructField("Nota3", FloatType(), True),
    StructField("Frequencia", IntegerType(), True)
])

# Criar um DataFrame vazio
df_alunos = spark.createDataFrame([], schema)

class Aluno:
    """Classe para representar um aluno."""
    def __init__(self, nome, nota1, nota2, nota3, frequencia):
        self.nome = nome
        self.notas = [nota1, nota2, nota3]
        self.frequencia = frequencia
        self.media = self.calcular_media()

    def calcular_media(self):
        """Calcula a média das notas do aluno."""
        return sum(self.notas) / len(self.notas)

class Escola:
    """Classe para gerenciar a lista de alunos usando PySpark."""
    def __init__(self):
        global df_alunos
        self.df_alunos = df_alunos

    def adicionar_aluno(self, aluno):
        """Adiciona um novo aluno ao DataFrame."""
        global df_alunos
        novo_aluno = [(aluno.nome, aluno.notas[0], aluno.notas[1], aluno.notas[2], aluno.frequencia)]
        df_novo_aluno = spark.createDataFrame(novo_aluno, schema)
        self.df_alunos = self.df_alunos.union(df_novo_aluno)
        df_alunos = self.df_alunos

    def modificar_aluno(self, nome, nota1=None, nota2=None, nota3=None, frequencia=None):
        """Modifica as informações de um aluno existente."""
        cond = self.df_alunos["Nome"] == nome
        if nota1 is not None:
            self.df_alunos = self.df_alunos.withColumn("Nota1", F.when(cond, nota1).otherwise(self.df_alunos["Nota1"]))
        if nota2 is not None:
            self.df_alunos = self.df_alunos.withColumn("Nota2", F.when(cond, nota2).otherwise(self.df_alunos["Nota2"]))
        if nota3 is not None:
            self.df_alunos = self.df_alunos.withColumn("Nota3", F.when(cond, nota3).otherwise(self.df_alunos["Nota3"]))
        if frequencia is not None:
            self.df_alunos = self.df_alunos.withColumn("Frequencia", F.when(cond, frequencia).otherwise(self.df_alunos["Frequencia"]))

    def listar_alunos(self):
        """Lista todos os alunos com suas informações."""
        self.df_alunos.show()

    def identificar_baixa_frequencia(self, limite=75):
        """Identifica alunos com baixa frequência."""
        self.df_alunos.filter(self.df_alunos["Frequencia"] < limite).show()

    def gerar_graficos(self):
        """Gera gráficos para visualização das notas e frequências dos alunos."""
        import matplotlib.pyplot as plt

        if self.df_alunos.count() == 0:
            print("Não há alunos cadastrados para gerar gráficos.")
            return

        alunos_pd = self.df_alunos.toPandas()
        nomes = alunos_pd['Nome']
        medias = (alunos_pd['Nota1'] + alunos_pd['Nota2'] + alunos_pd['Nota3']) / 3
        frequencias = alunos_pd['Frequencia']

        # Gráfico de médias das notas
        plt.figure(figsize=(12, 6))
        plt.subplot(1, 2, 1)
        plt.bar(nomes, medias, color='orange')
        plt.title('Médias das Notas dos Alunos')
        plt.xlabel('Nome')
        plt.ylabel('Média')
        plt.xticks(rotation=45)

        # Gráfico de frequências
        plt.subplot(1, 2, 2)
        plt.bar(nomes, frequencias, color='blue')
        plt.title('Frequência dos Alunos')
        plt.xlabel('Nome')
        plt.ylabel('Frequência (%)')
        plt.xticks(rotation=45)

        plt.tight_layout()
        plt.show()

class Sistema:
    """Classe principal do sistema que interage com o usuário."""
    def __init__(self):
        self.escola = Escola()

    def menu(self):
        """Menu interativo do sistema."""
        print("=== Sistema de Gerenciamento Escolar ===")
        print("1. Adicionar Aluno")
        print("2. Modificar Aluno")
        print("3. Listar Alunos")
        print("4. Identificar Alunos com Baixa Frequência")
        print("5. Gerar Gráficos")
        print("0. Sair")
        
        opcao = input("Escolha uma opção: ")
        return opcao

    def executar(self):
        """Método para executar o sistema."""
        while True:
            opcao = self.menu()
            
            if opcao == '1':
                nome = input("Nome do aluno: ")
                nota1 = float(input("Nota 1: "))
                nota2 = float(input("Nota 2: "))
                nota3 = float(input("Nota 3: "))
                frequencia = float(input("Frequência (%): "))
                aluno = Aluno(nome, nota1, nota2, nota3, frequencia)
                self.escola.adicionar_aluno(aluno)
                print(f"Aluno {nome} adicionado com sucesso!")

            elif opcao == '2':
                nome = input("Nome do aluno a ser modificado: ")
                nota1 = input("Nova Nota 1 (deixe em branco para não alterar): ")
                nota2 = input("Nova Nota 2 (deixe em branco para não alterar): ")
                nota3 = input("Nova Nota 3 (deixe em branco para não alterar): ")
                frequencia = input("Nova Frequência (%) (deixe em branco para não alterar): ")
                
                nota1 = float(nota1) if nota1 else None
                nota2 = float(nota2) if nota2 else None
                nota3 = float(nota3) if nota3 else None
                frequencia = float(frequencia) if frequencia else None

                self.escola.modificar_aluno(nome, nota1, nota2, nota3, frequencia)
                print(f"Informações do aluno {nome} modificadas com sucesso!")

            elif opcao == '3':
                print("Lista de Alunos:")
                self.escola.listar_alunos()

            elif opcao == '4':
                print("Alunos com baixa frequência:")
                self.escola.identificar_baixa_frequencia()

            elif opcao == '5':
                self.escola.gerar_graficos()

            elif opcao == '0':
                print("Saindo do sistema.")
                break
            
            else:
                print("Opção inválida. Tente novamente.")

if __name__ == "__main__":
    sistema = Sistema()
    sistema.executar()

# Parar a sessão Spark
spark.stop()
git config --global user.name Samuca291
git config --global user.email toledosamuel400@gmail.com

