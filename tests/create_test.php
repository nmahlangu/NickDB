<?php

	// template
	$prefix = "create(";
	$suffix = ',"unsorted")';
	$insert = "t";
	$letter = 97;

	// do all 26 letters
	for ($j = 1; $j < 100; $j++)
	{
		for ($i = 0; $i < 26; $i++)
			echo $prefix . $insert . $j . chr($letter + $i) . $suffix . PHP_EOL;
	}
	echo "Quit\n";