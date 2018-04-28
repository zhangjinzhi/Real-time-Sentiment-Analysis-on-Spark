
def changeData(original_fileName,new_fileName):
    
    with open(new_fileName, 'wt') as f_new:
        number = 0

        f_original = open(original_fileName)

        for line in f_original.readlines():
            text = line.strip()

            if len(text.split()) == 0:
                continue

            context = str(number) +'\t'+text

            print(context, file=f_new)

            number += 1







if __name__ == '__main__':

    original_fileName = 'total.txt'
    new_fileName = 'total_with_index.txt'
    # # new_fileName = '../test/new_test.txt'

    changeData(original_fileName,new_fileName)

 

